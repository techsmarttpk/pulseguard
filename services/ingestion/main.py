"""Ingestion service: consumes raw market-data events from Kafka, runs
deterministic validation, measures per-symbol latency/throughput, and
routes bad/invalid/corrupted payloads to the dead-letter topic. This is the
first stage in the pipeline — anomaly detection (statistical + Isolation
Forest) happens downstream in the anomaly_detection service, which consumes
the same market-data topic on its own consumer group.
"""
from __future__ import annotations

import asyncio
import signal
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "common"))

from prometheus_client import start_http_server  # noqa: E402

from pulseguard_common.alerting import (  # noqa: E402
    AlertDeduplicator,
    build_alert,
    emit_alert,
    make_dedup_key,
)
from pulseguard_common.db import make_pool  # noqa: E402
from pulseguard_common.kafka_utils import (  # noqa: E402
    deserialize,
    make_consumer,
    make_producer,
    periodic_lag_reporter,
    run_concurrent_consumer,
    send_to_dead_letter,
)
from pulseguard_common.logging_utils import configure_logging  # noqa: E402
from pulseguard_common.metrics import build_core_metrics  # noqa: E402
from pulseguard_common.stats import RateCounter, RollingWindow  # noqa: E402

from config import load_config  # noqa: E402
from validation import ValidationEngine  # noqa: E402


class IngestionService:
    def __init__(self, cfg):
        self.cfg = cfg
        self.log = configure_logging("ingestion", cfg.log_level)
        self.metrics = build_core_metrics()
        self.validator = ValidationEngine(
            stale_threshold_seconds=cfg.stale_threshold_seconds,
            extreme_quantity_threshold=cfg.extreme_quantity_threshold,
            duplicate_cache_size=cfg.duplicate_cache_size,
        )
        self.dedup = AlertDeduplicator(cooldown_seconds=cfg.alert_cooldown_seconds)
        self.latency_windows: dict[str, RollingWindow] = {}
        self.rate_counters: dict[str, RateCounter] = {}
        self.pool = None
        self.producer = None
        self.consumer = None

    def _latency_window(self, symbol: str) -> RollingWindow:
        return self.latency_windows.setdefault(symbol, RollingWindow(maxlen=1000))

    def _rate_counter(self, symbol: str) -> RateCounter:
        return self.rate_counters.setdefault(symbol, RateCounter(window_seconds=10.0))

    async def handle_message(self, raw: bytes) -> None:
        start = time.perf_counter()
        try:
            event = deserialize(raw)
        except Exception as exc:  # noqa: BLE001
            self.metrics.messages_rejected_total.labels(feed="unknown", reason="deserialize_error").inc()
            await send_to_dead_letter(self.producer, self.cfg.topic_dead_letter, raw, f"deserialize_error: {exc}")
            return

        # Corrupted-but-parseable payloads (missing required fields).
        required = ("event_id", "symbol", "price", "bid", "ask", "sequence_number", "producer_timestamp")
        missing = [f for f in required if f not in event]
        if missing:
            symbol = event.get("symbol", "unknown")
            self.metrics.messages_rejected_total.labels(feed=symbol, reason="missing_fields").inc()
            await send_to_dead_letter(
                self.producer, self.cfg.topic_dead_letter, event, f"missing_fields: {missing}"
            )
            return

        symbol = event["symbol"]
        self.metrics.messages_received_total.labels(feed=symbol).inc()
        self._rate_counter(symbol).tick()

        now = time.time()
        latency = max(0.0, now - float(event["producer_timestamp"]))
        self.metrics.market_data_latency_seconds.labels(feed=symbol).observe(latency)
        self._latency_window(symbol).add(latency)

        result = self.validator.validate(event, now=now)

        if result.is_duplicate:
            self.metrics.duplicate_events_total.labels(feed=symbol).inc()
            await self._maybe_alert(
                "LOW", "DUPLICATE_EVENT", symbol, f"Duplicate event_id detected for {symbol}",
                {"event_id": event["event_id"]},
            )

        if result.is_sequence_gap:
            await self._maybe_alert(
                "MEDIUM", "SEQUENCE_GAP", symbol,
                f"Sequence gap of {result.gap_size} message(s) detected for {symbol}",
                {"gap_size": result.gap_size, "sequence_number": event["sequence_number"]},
            )

        if result.is_stale:
            self.metrics.messages_rejected_total.labels(feed=symbol, reason="stale_event").inc()

        if not result.is_valid:
            for reason in result.reasons:
                if reason in ("duplicate_event_id", "stale_event"):
                    continue
                self.metrics.messages_rejected_total.labels(feed=symbol, reason=reason).inc()
            await send_to_dead_letter(
                self.producer, self.cfg.topic_dead_letter, event, ",".join(result.reasons)
            )
            severity = "HIGH" if "extreme_quantity" not in result.reasons else "MEDIUM"
            await self._maybe_alert(
                severity, "INVALID_EVENT", symbol,
                f"Invalid market event for {symbol}: {', '.join(result.reasons)}",
                {"reasons": result.reasons, "price": event.get("price"), "bid": event.get("bid"), "ask": event.get("ask")},
            )
        else:
            self.metrics.messages_processed_total.labels(feed=symbol).inc()

        self.metrics.processing_latency_seconds.labels(stage="ingestion").observe(time.perf_counter() - start)

    async def _maybe_alert(self, severity: str, alert_type: str, symbol: str, description: str, metrics: dict):
        dedup_key = make_dedup_key(alert_type, symbol, symbol)
        if not self.dedup.should_fire(dedup_key):
            return
        alert = build_alert(
            severity=severity,
            alert_type=alert_type,
            feed=symbol,
            description=description,
            detection_source="VALIDATION_ENGINE",
            symbol=symbol,
            metrics=metrics,
        )
        await emit_alert(self.pool, self.producer, self.cfg.topic_alerts, alert, log=self.log)

    async def run(self):
        start_http_server(self.cfg.metrics_port)
        self.log.info("metrics_server_started", port=self.cfg.metrics_port)

        self.pool = await make_pool(self.cfg.database_url)
        self.producer = await make_producer(self.cfg.kafka_bootstrap_servers)
        self.consumer = make_consumer(
            [self.cfg.topic_market_data], self.cfg.consumer_group, self.cfg.kafka_bootstrap_servers
        )
        await self.consumer.start()
        self.log.info(
            "ingestion_started",
            topic=self.cfg.topic_market_data,
            group=self.cfg.consumer_group,
            concurrency=self.cfg.consumer_concurrency,
        )

        lag_task = asyncio.create_task(
            periodic_lag_reporter(
                self.consumer, self.metrics.kafka_consumer_lag, self.cfg.topic_market_data, self.cfg.consumer_group
            )
        )

        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop_event.set)
            except NotImplementedError:
                pass

        consume_task = asyncio.create_task(
            run_concurrent_consumer(self.consumer, self.handle_message, concurrency=self.cfg.consumer_concurrency)
        )

        await stop_event.wait()
        self.log.info("ingestion_shutting_down")
        consume_task.cancel()
        lag_task.cancel()
        for t in (consume_task, lag_task):
            try:
                await t
            except asyncio.CancelledError:
                pass
        await self.consumer.stop()
        await self.producer.stop()
        await self.pool.close()
        self.log.info("ingestion_stopped")


async def main():
    cfg = load_config()
    service = IngestionService(cfg)
    await service.run()


if __name__ == "__main__":
    asyncio.run(main())
