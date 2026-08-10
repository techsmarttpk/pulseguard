#!/usr/bin/env python3
"""PulseGuard live pipeline benchmark/sampler.

Samples the REAL, running pipeline via Prometheus (and optionally `docker
stats` for CPU/memory) — this is the counterpart to
`scripts/benchmark_hotpath.py`, which measures the application code's raw
capacity in isolation. Use this one to get real producer rate, consumer
rate, per-consumer-group lag, latency percentiles, and container CPU/
memory while the actual Kafka/Postgres/services stack is running.

Basic usage (stack already running via `docker compose up`):

    python scripts/benchmark.py --duration 60

Full controlled-rate benchmark (edits .env, recreates the simulator
container at each rate, and samples): see scripts/rate_sweep.sh for
running this across multiple target rates (100/500/1000/5000) in one go
and getting a comparison table.

No network access beyond your local Prometheus (http://localhost:9090 by
default) is required. Nothing here is simulated or invented — every number
printed is either a live Prometheus query result or, for --docker-stats,
literally parsed from `docker stats`.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request

DOCKER_SERVICES = [
    "pulseguard-simulator",
    "pulseguard-ingestion",
    "pulseguard-anomaly-detection",
    "pulseguard-monitoring",
    "pulseguard-kafka",
    "pulseguard-postgres",
]


def query(prom_url: str, expr: str) -> float:
    """Instant PromQL query, returns the first result's value as a float."""
    url = f"{prom_url}/api/v1/query?query={urllib.parse.quote(expr)}"
    with urllib.request.urlopen(url, timeout=5) as resp:
        data = json.loads(resp.read())
    result = data.get("data", {}).get("result", [])
    if not result:
        return 0.0
    try:
        return float(result[0]["value"][1])
    except (KeyError, IndexError, ValueError):
        return 0.0


def query_series(prom_url: str, expr: str) -> list[dict]:
    """Instant PromQL query, returns every series as {labels, value}."""
    url = f"{prom_url}/api/v1/query?query={urllib.parse.quote(expr)}"
    with urllib.request.urlopen(url, timeout=5) as resp:
        data = json.loads(resp.read())
    out = []
    for item in data.get("data", {}).get("result", []):
        try:
            out.append({"labels": item.get("metric", {}), "value": float(item["value"][1])})
        except (KeyError, IndexError, ValueError):
            continue
    return out


def set_simulator_rate(rate: int, env_path: str = ".env") -> None:
    """Rewrites SIMULATOR_THROUGHPUT_MSG_PER_SEC in .env and recreates just
    the simulator container so the new rate takes effect. Requires the
    `docker` CLI on PATH and to be run from the repo root (where .env and
    docker-compose.yml live)."""
    with open(env_path, "r", encoding="utf-8") as f:
        content = f.read()
    if re.search(r"^SIMULATOR_THROUGHPUT_MSG_PER_SEC=.*$", content, flags=re.MULTILINE):
        content = re.sub(
            r"^SIMULATOR_THROUGHPUT_MSG_PER_SEC=.*$",
            f"SIMULATOR_THROUGHPUT_MSG_PER_SEC={rate}",
            content,
            flags=re.MULTILINE,
        )
    else:
        content += f"\nSIMULATOR_THROUGHPUT_MSG_PER_SEC={rate}\n"
    with open(env_path, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"Set SIMULATOR_THROUGHPUT_MSG_PER_SEC={rate} in {env_path}, recreating simulator container...")
    subprocess.run(["docker", "compose", "up", "-d", "--force-recreate", "simulator"], check=True)


def docker_stats(container_names: list[str]) -> dict[str, dict]:
    """Runs `docker stats --no-stream` once and returns {name: {cpu_pct, mem_usage}}."""
    try:
        out = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"]
            + container_names,
            capture_output=True, text=True, timeout=15,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        print(f"  (docker stats unavailable: {exc})")
        return {}
    stats = {}
    for line in out.stdout.strip().splitlines():
        parts = line.split("\t")
        if len(parts) == 3:
            name, cpu, mem = parts
            stats[name] = {"cpu_pct": cpu, "mem_usage": mem}
    return stats


def sample_once(prom_url: str, window: str = "30s") -> dict:
    row = {
        "received_per_sec": query(prom_url, f"sum(rate(pulseguard_messages_received_total[{window}]))"),
        "processed_per_sec": query(prom_url, f"sum(rate(pulseguard_messages_processed_total[{window}]))"),
        "rejected_per_sec": query(prom_url, f"sum(rate(pulseguard_messages_rejected_total[{window}]))"),
        "produced_per_sec": query(prom_url, f"sum(pulseguard_feed_messages_per_second)"),
        "p50_ms": query(prom_url, f"histogram_quantile(0.50, sum(rate(pulseguard_market_data_latency_seconds_bucket[{window}])) by (le)) * 1000"),
        "p95_ms": query(prom_url, f"histogram_quantile(0.95, sum(rate(pulseguard_market_data_latency_seconds_bucket[{window}])) by (le)) * 1000"),
        "p99_ms": query(prom_url, f"histogram_quantile(0.99, sum(rate(pulseguard_market_data_latency_seconds_bucket[{window}])) by (le)) * 1000"),
        # NOT summed across consumer groups on purpose — see
        # api/routers/metrics.py for why sum() across independent groups
        # reading the same topic is misleading. max() = worst-lagging
        # group; the per-group breakdown below shows all of them.
        "consumer_lag_max": query(prom_url, "max(pulseguard_kafka_consumer_lag)"),
        "consumer_lag_by_group": query_series(prom_url, "pulseguard_kafka_consumer_lag"),
    }
    return row


def print_sample(row: dict) -> None:
    print(
        f"  produced={row['produced_per_sec']:.0f}/s recv={row['received_per_sec']:.0f}/s "
        f"proc={row['processed_per_sec']:.0f}/s rej={row['rejected_per_sec']:.0f}/s "
        f"p50={row['p50_ms']:.1f}ms p95={row['p95_ms']:.1f}ms p99={row['p99_ms']:.1f}ms "
        f"lag(max)={row['consumer_lag_max']:.0f}"
    )
    for series in row["consumer_lag_by_group"]:
        labels = series["labels"]
        print(f"      lag[{labels.get('group','?')} / {labels.get('topic','?')}] = {series['value']:.0f}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--prometheus-url", default="http://localhost:9090")
    parser.add_argument("--duration", type=int, default=60, help="seconds to sample")
    parser.add_argument("--interval", type=int, default=5, help="seconds between samples")
    parser.add_argument("--set-rate", type=int, default=None,
                         help="If given, first rewrites .env's SIMULATOR_THROUGHPUT_MSG_PER_SEC and "
                              "recreates the simulator container (requires docker CLI + running from repo root)")
    parser.add_argument("--warmup", type=int, default=15, help="seconds to wait after --set-rate before sampling")
    parser.add_argument("--docker-stats", action="store_true",
                         help="Also capture `docker stats` CPU/memory for PulseGuard containers")
    args = parser.parse_args()

    if args.set_rate is not None:
        set_simulator_rate(args.set_rate)
        print(f"Warming up for {args.warmup}s...")
        time.sleep(args.warmup)

    samples = []
    end = time.time() + args.duration
    print(f"Sampling PulseGuard metrics from {args.prometheus_url} for {args.duration}s...")
    while time.time() < end:
        row = sample_once(args.prometheus_url)
        samples.append(row)
        print_sample(row)
        time.sleep(args.interval)

    if not samples:
        print("No samples collected.")
        sys.exit(1)

    def avg(key):
        return sum(s[key] for s in samples) / len(samples)

    def mx(key):
        return max(s[key] for s in samples)

    print("\n--- Summary ---")
    for key in ("produced_per_sec", "received_per_sec", "processed_per_sec", "rejected_per_sec",
                "p50_ms", "p95_ms", "p99_ms", "consumer_lag_max"):
        print(f"{key:>18}: avg={avg(key):.1f}  max={mx(key):.1f}")

    if args.docker_stats:
        print("\n--- docker stats (point-in-time, at end of run) ---")
        stats = docker_stats(DOCKER_SERVICES)
        if not stats:
            print("  (no data — is the docker CLI available and are these containers running?)")
        for name, s in stats.items():
            print(f"  {name:<32} cpu={s['cpu_pct']:<8} mem={s['mem_usage']}")


if __name__ == "__main__":
    main()
