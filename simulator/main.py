"""PulseGuard market data simulator entrypoint.

Design notes:
- Throughput is controlled by a tick-based scheduler (default 50ms ticks),
  not a per-message `sleep()` — at 50,000 msg/sec a per-message sleep is
  both inaccurate (sleep() resolution) and wastes the event loop. Instead we
  compute how many messages are due each tick and fire them concurrently.
- Kafka delivery uses `send()` (batched by aiokafka's internal accumulator
  via linger_ms) rather than `send_and_wait()` on the hot path, so producer
  throughput isn't bottlenecked on a broker round-trip per message.
  Delivery failures are surfaced via the futures' callbacks and logged.
  The producer itself (see `pulseguard_common.kafka_utils.make_producer`)
  still runs with `acks=-1` + `enable_idempotence=True` — not awaiting the
  future synchronously is what buys throughput here, not weaker delivery
  guarantees. See README "Engineering trade-offs".
"""
from __future__ import annotations

import asyncio
import itertools
import random
import signal
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "services" / "common"))

from pulseguard_common.kafka_utils import make_producer, serialize  # noqa: E402
from pulseguard_common.logging_utils import configure_logging  # noqa: E402
from pulseguard_common.models import EventType, new_id  # noqa: E402

from config import load_config  # noqa: E402
from generator import SymbolState  # noqa: E402
from injector import EpisodicState, PerMessageInjector  # noqa: E402

TICK_SECONDS = 0.05  # 20 ticks/sec scheduling resolution


class Simulator:
    def __init__(self, cfg):
        self.cfg = cfg
        self.log = configure_logging("simulator")
        self.symbols = {s: SymbolState.new(s) for s in cfg.symbol_list}
        self.exchanges = cfg.exchange_list
        self.sequence_counters = {s: itertools.count(1) for s in cfg.symbol_list}
        self.last_event_by_symbol: dict[str, dict] = {}
        self.injector = PerMessageInjector(cfg)
        self.episodic = EpisodicState(cfg)
        self.producer = None
        self._stop = asyncio.Event()
        self._sent = 0
        self._dropped = 0
        self._injected_counts: dict[str, int] = {}
        self._delayed_tasks: set[asyncio.Task] = set()

    async def start(self):
        self.producer = await make_producer(self.cfg.kafka_bootstrap_servers)
        self.log.info(
            "simulator_started",
            symbols=self.cfg.symbol_list,
            throughput=self.cfg.throughput_msg_per_sec,
            topic=self.cfg.kafka_topic_market_data,
        )

    async def stop(self):
        self._stop.set()
        if self._delayed_tasks:
            await asyncio.wait(self._delayed_tasks)
        if self.producer:
            await self.producer.stop()
        self.log.info("simulator_stopped", total_sent=self._sent, total_dropped=self._dropped)

    def _build_event(self, symbol: str) -> dict:
        state = self.symbols[symbol]
        state.step()
        seq = next(self.sequence_counters[symbol])
        event_type = random.choices(
            [EventType.TRADE.value, EventType.QUOTE.value, EventType.BID_ASK.value],
            weights=[0.5, 0.3, 0.2],
        )[0]
        event = {
            "event_id": new_id("evt_"),
            "sequence_number": seq,
            "symbol": symbol,
            "exchange": random.choice(self.exchanges),
            "event_type": event_type,
            "price": state.price,
            "quantity": state.sample_quantity(),
            "bid": state.last_bid,
            "ask": state.last_ask,
            "producer_timestamp": time.time(),
            "metadata": {},
        }
        return event

    async def _deliver(self, event: dict, payload: bytes | None = None):
        topic = self.cfg.kafka_topic_market_data
        data = payload if payload is not None else serialize(event)
        key = event["symbol"].encode() if "symbol" in event else None
        try:
            # Fire-and-forget on the hot path; aiokafka batches internally.
            fut = await self.producer.send(topic, value=data, key=key)
            fut.add_done_callback(self._on_delivered)
        except Exception as exc:  # noqa: BLE001
            self.log.warning("producer_send_failed", error=str(exc))
            self._dropped += 1

    def _on_delivered(self, fut: asyncio.Future):
        if fut.exception() is not None:
            self._dropped += 1
        else:
            self._sent += 1

    async def _emit_symbol_event(self, symbol: str):
        event = self._build_event(symbol)
        outcome = self.injector.apply(event)

        if outcome.injected_kind:
            self._injected_counts[outcome.injected_kind] = (
                self._injected_counts.get(outcome.injected_kind, 0) + 1
            )

        if outcome.action == "corrupt":
            await self._deliver(event, payload=outcome.corrupted_payload)
            return

        if outcome.action == "drop":
            self._dropped += 1
            return

        if outcome.delay_seconds > 0:
            task = asyncio.create_task(self._delayed_send(event, outcome.delay_seconds))
            self._delayed_tasks.add(task)
            task.add_done_callback(self._delayed_tasks.discard)
            return

        await self._deliver(event)
        self.last_event_by_symbol[symbol] = event

        if outcome.action == "duplicate":
            # Re-send the identical event (same event_id + sequence_number)
            # shortly after, exercising duplicate-detection downstream.
            await asyncio.sleep(random.uniform(0.01, 0.2))
            await self._deliver(dict(event))

    async def _delayed_send(self, event: dict, delay_seconds: float):
        await asyncio.sleep(delay_seconds)
        await self._deliver(event)

    async def run(self):
        await self.start()
        start_time = time.time()
        next_stats_log = start_time + 5.0

        while not self._stop.is_set():
            tick_start = time.time()
            self.episodic.tick(tick_start)
            multiplier = self.episodic.throughput_multiplier(tick_start)

            target_per_tick = self.cfg.throughput_msg_per_sec * multiplier * TICK_SECONDS
            whole = int(target_per_tick)
            frac = target_per_tick - whole
            n_events = whole + (1 if random.random() < frac else 0)

            if n_events > 0:
                symbols = self.cfg.symbol_list
                tasks = [
                    self._emit_symbol_event(random.choice(symbols)) for _ in range(n_events)
                ]
                await asyncio.gather(*tasks, return_exceptions=True)

            if tick_start >= next_stats_log:
                self.log.info(
                    "simulator_stats",
                    sent_total=self._sent,
                    dropped_total=self._dropped,
                    episodic_status=self.episodic.status(tick_start),
                    injected_counts=dict(self._injected_counts),
                )
                next_stats_log = tick_start + 5.0

            if self.cfg.run_duration_seconds and (tick_start - start_time) >= self.cfg.run_duration_seconds:
                break

            elapsed = time.time() - tick_start
            await asyncio.sleep(max(0.0, TICK_SECONDS - elapsed))

        await self.stop()


async def main():
    cfg = load_config()
    sim = Simulator(cfg)

    loop = asyncio.get_running_loop()

    def _request_stop():
        sim._stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except NotImplementedError:
            pass  # Windows fallback; Ctrl+C still raises KeyboardInterrupt

    await sim.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
