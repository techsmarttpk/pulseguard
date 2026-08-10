#!/usr/bin/env python3
"""In-process capacity benchmark for PulseGuard's CPU-bound hot-path code —
the market-event generator/injector and the ingestion validation engine —
run directly against the real modules with Kafka/Postgres/network entirely
removed from the picture.

This answers a different, narrower question than `scripts/benchmark.py`
(which samples the live, deployed pipeline via Prometheus): "is the Python
code itself fast enough to sustain N msg/sec on one CPU core, or is the
bottleneck somewhere else (network, broker, DB, container resource
limits)?" Use both together when diagnosing a throughput/latency problem —
if this script easily clears a target rate but the live deployment can't,
the bottleneck is infra/config, not the request-handling code.

Usage:
    python scripts/benchmark_hotpath.py --rates 100 500 1000 5000 --duration 3
"""
from __future__ import annotations

import argparse
import asyncio
import resource
import sys
import time
from pathlib import Path
from statistics import mean

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "services" / "common"))
sys.path.insert(0, str(ROOT / "simulator"))
sys.path.insert(0, str(ROOT / "services" / "ingestion"))

from generator import SymbolState  # noqa: E402
from injector import PerMessageInjector  # noqa: E402
from validation import ValidationEngine  # noqa: E402
from pulseguard_common.kafka_utils import serialize  # noqa: E402

SYMBOLS = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "TSLA", "META"]


class _StubConfig:
    """Matches the subset of SimulatorConfig fields PerMessageInjector
    reads, at PulseGuard's shipped default injection probabilities."""

    inject_enabled = True
    inject_price_spike_prob = 0.0008
    inject_price_crash_prob = 0.0008
    inject_negative_price_prob = 0.0003
    inject_zero_price_prob = 0.0003
    inject_bad_bid_ask_prob = 0.0005
    inject_extreme_quantity_prob = 0.0005
    inject_duplicate_prob = 0.0008
    inject_sequence_gap_prob = 0.0008
    inject_stale_event_prob = 0.0005
    inject_delayed_event_prob = 0.0008
    inject_corrupted_event_prob = 0.0003


def percentile(sorted_values: list[float], p: float) -> float:
    if not sorted_values:
        return 0.0
    k = (len(sorted_values) - 1) * (p / 100)
    f, c = int(k), min(int(k) + 1, len(sorted_values) - 1)
    if f == c:
        return sorted_values[f]
    return sorted_values[f] + (sorted_values[c] - sorted_values[f]) * (k - f)


def run_generation_benchmark(duration_s: float) -> dict:
    """How fast can we generate+inject events (the simulator's own hot
    path), single-threaded, synchronously?"""
    states = {s: SymbolState.new(s) for s in SYMBOLS}
    injector = PerMessageInjector(_StubConfig())
    seq = {s: 0 for s in SYMBOLS}

    latencies = []
    count = 0
    start = time.perf_counter()
    end = start + duration_s
    i = 0
    while time.perf_counter() < end:
        symbol = SYMBOLS[i % len(SYMBOLS)]
        i += 1
        t0 = time.perf_counter()
        state = states[symbol]
        state.step()
        seq[symbol] += 1
        event = {
            "event_id": f"evt_{count}",
            "sequence_number": seq[symbol],
            "symbol": symbol,
            "exchange": "NASDAQ",
            "event_type": "TRADE",
            "price": state.price,
            "quantity": state.sample_quantity(),
            "bid": state.last_bid,
            "ask": state.last_ask,
            "producer_timestamp": time.time(),
            "metadata": {},
        }
        injector.apply(event)
        latencies.append(time.perf_counter() - t0)
        count += 1
    elapsed = time.perf_counter() - start
    latencies.sort()
    return {
        "stage": "simulator generation+injection",
        "count": count,
        "elapsed_s": elapsed,
        "achieved_msg_per_sec": count / elapsed,
        "p50_us": percentile(latencies, 50) * 1e6,
        "p95_us": percentile(latencies, 95) * 1e6,
        "p99_us": percentile(latencies, 99) * 1e6,
    }


def run_validation_benchmark(duration_s: float) -> dict:
    """How fast can the ingestion ValidationEngine process pre-generated
    events (the ingestion consumer's own hot path, minus Kafka/DB I/O)?"""
    engine = ValidationEngine()
    states = {s: SymbolState.new(s) for s in SYMBOLS}
    injector = PerMessageInjector(_StubConfig())
    seq = {s: 0 for s in SYMBOLS}

    latencies = []
    count = 0
    start = time.perf_counter()
    end = start + duration_s
    i = 0
    while time.perf_counter() < end:
        symbol = SYMBOLS[i % len(SYMBOLS)]
        i += 1
        state = states[symbol]
        state.step()
        seq[symbol] += 1
        event = {
            "event_id": f"evt_{count}",
            "sequence_number": seq[symbol],
            "symbol": symbol,
            "exchange": "NASDAQ",
            "event_type": "TRADE",
            "price": state.price,
            "quantity": state.sample_quantity(),
            "bid": state.last_bid,
            "ask": state.last_ask,
            "producer_timestamp": time.time(),
            "metadata": {},
        }
        injector.apply(event)

        t0 = time.perf_counter()
        engine.validate(event)
        latencies.append(time.perf_counter() - t0)
        count += 1
    elapsed = time.perf_counter() - start
    latencies.sort()
    return {
        "stage": "ingestion validation (no Kafka/DB I/O)",
        "count": count,
        "elapsed_s": elapsed,
        "achieved_msg_per_sec": count / elapsed,
        "p50_us": percentile(latencies, 50) * 1e6,
        "p95_us": percentile(latencies, 95) * 1e6,
        "p99_us": percentile(latencies, 99) * 1e6,
    }


class _FakePool:
    """No-op stand-in for the asyncpg pool, so emit_alert()'s DB write
    resolves instantly instead of needing a real Postgres connection."""

    async def execute(self, *args, **kwargs):
        return None


class _FakeProducer:
    """No-op stand-in for the aiokafka producer, so alert/dead-letter
    publishes resolve instantly instead of needing a real broker."""

    async def send_and_wait(self, *args, **kwargs):
        return None


async def _run_ingestion_handler_benchmark_async(duration_s: float, concurrency: int) -> dict:
    """Exercises the REAL IngestionService.handle_message() coroutine —
    orjson deserialize, validation, Prometheus counter/histogram updates,
    dedup-gated alert emission — with fake (instant) Kafka/DB calls, at the
    same concurrency the live service uses. This is the most faithful
    in-process approximation of ingestion's actual per-message cost
    available without a real broker.
    """
    sys.path.insert(0, str(ROOT / "services" / "ingestion"))
    import importlib

    ingestion_config = importlib.import_module("config")
    ingestion_main = importlib.import_module("main")

    cfg = ingestion_config.IngestionConfig()
    service = ingestion_main.IngestionService(cfg)
    service.pool = _FakePool()
    service.producer = _FakeProducer()

    states = {s: SymbolState.new(s) for s in SYMBOLS}
    injector = PerMessageInjector(_StubConfig())
    seq = {s: 0 for s in SYMBOLS}

    def _next_payload(i: int) -> bytes:
        symbol = SYMBOLS[i % len(SYMBOLS)]
        state = states[symbol]
        state.step()
        seq[symbol] += 1
        event = {
            "event_id": f"evt_{i}",
            "sequence_number": seq[symbol],
            "symbol": symbol,
            "exchange": "NASDAQ",
            "event_type": "TRADE",
            "price": state.price,
            "quantity": state.sample_quantity(),
            "bid": state.last_bid,
            "ask": state.last_ask,
            "producer_timestamp": time.time(),
            "metadata": {},
        }
        injector.apply(event)
        return serialize(event)

    latencies = []
    count = 0
    start = time.perf_counter()
    end = start + duration_s
    i = 0
    while time.perf_counter() < end:
        batch = [_next_payload(i + j) for j in range(concurrency)]
        i += concurrency

        async def _timed(payload):
            t0 = time.perf_counter()
            await service.handle_message(payload)
            return time.perf_counter() - t0

        results = await asyncio.gather(*(_timed(p) for p in batch))
        latencies.extend(results)
        count += len(batch)
    elapsed = time.perf_counter() - start
    latencies.sort()
    return {
        "stage": f"ingestion handle_message() end-to-end, async, concurrency={concurrency} (fake Kafka/DB)",
        "count": count,
        "elapsed_s": elapsed,
        "achieved_msg_per_sec": count / elapsed,
        "p50_us": percentile(latencies, 50) * 1e6,
        "p95_us": percentile(latencies, 95) * 1e6,
        "p99_us": percentile(latencies, 99) * 1e6,
    }


def run_ingestion_handler_benchmark(duration_s: float, concurrency: int = 4) -> dict:
    return asyncio.run(_run_ingestion_handler_benchmark_async(duration_s, concurrency))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rates", type=int, nargs="+", default=[100, 500, 1000, 5000],
                         help="Target rates to evaluate against the measured max capacity (msg/sec)")
    parser.add_argument("--duration", type=float, default=3.0, help="Seconds to run each stage")
    args = parser.parse_args()

    rusage_before = resource.getrusage(resource.RUSAGE_SELF)
    cpu_before = rusage_before.ru_utime + rusage_before.ru_stime

    gen_result = run_generation_benchmark(args.duration)
    val_result = run_validation_benchmark(args.duration)
    handler_result = run_ingestion_handler_benchmark(args.duration, concurrency=4)

    rusage_after = resource.getrusage(resource.RUSAGE_SELF)
    cpu_after = rusage_after.ru_utime + rusage_after.ru_stime

    print("=" * 78)
    print("PulseGuard hot-path capacity benchmark (single core, in-process, no Kafka/DB)")
    print("=" * 78)
    for r in (gen_result, val_result, handler_result):
        print(f"\n[{r['stage']}]")
        print(f"  processed        : {r['count']} events in {r['elapsed_s']:.2f}s")
        print(f"  max throughput   : {r['achieved_msg_per_sec']:.0f} msg/sec (this stage alone, single core)")
        print(f"  per-event p50/p95/p99: {r['p50_us']:.1f}us / {r['p95_us']:.1f}us / {r['p99_us']:.1f}us")

    print(f"\nProcess CPU time consumed by this benchmark: {cpu_after - cpu_before:.2f}s")
    print(f"Peak RSS (this process): {rusage_after.ru_maxrss / 1024:.1f} MB")

    print("\n--- Target rate feasibility (end-to-end handle_message(), concurrency=4) ---")
    max_rate = handler_result["achieved_msg_per_sec"]
    for rate in sorted(args.rates):
        headroom = max_rate / rate if rate else float("inf")
        verdict = "OK, comfortable headroom" if headroom > 3 else ("OK, tight" if headroom >= 1 else "WOULD NOT KEEP UP")
        print(f"  {rate:>6} msg/sec -> requires {100/headroom:.1f}% of one core's capacity -> {verdict}")

    print(
        "\nNote: this measures ONE core's raw Python throughput for the "
        "CPU-bound part of the pipeline only. It deliberately excludes "
        "Kafka network I/O, broker fsync/replication, and Postgres writes — "
        "those only happen on a small, alert-cooldown-gated fraction of "
        "messages. If the live deployment can't sustain a rate this script "
        "clears easily, the bottleneck is infrastructure/config (consumer "
        "concurrency, broker resources, Docker Desktop resource limits on "
        "Windows, etc.), not this code."
    )


if __name__ == "__main__":
    main()
