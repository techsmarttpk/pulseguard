"""Monitoring service: feed health state machine, latency/throughput
tracking, and health-driven alerting.

This service intentionally maintains its OWN lightweight view of the
stream (via a dedicated consumer group on market-data) rather than reading
another service's in-memory state, and its own view of recent alert volume
(via a dedicated consumer group on alerts) to compute an error/anomaly
rate per feed. That keeps every service independently deployable/scalable,
consistent with the rest of the pipeline.
"""
from __future__ import annotations

import asyncio
import signal
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "common"))

from prometheus_client import start_http_server  # noqa: E402

from pulseguard_common.alerting import AlertDeduplicator, build_alert, emit_alert, make_dedup_key  # noqa: E402
from pulseguard_common.db import (  # noqa: E402
    insert_feed_status_transition,
    make_pool,
    resolve_alerts_by_dedup_key,
    upsert_aggregated_metrics,
)
from pulseguard_common.kafka_utils import (  # noqa: E402
    deserialize,
    get_total_consumer_lag,
    make_consumer,
    make_producer,
    run_concurrent_consumer,
)
from pulseguard_common.logging_utils import configure_logging  # noqa: E402
from pulseguard_common.metrics import FEED_HEALTH_TO_INT, build_core_metrics  # noqa: E402
from pulseguard_common.stats import RateCounter, RollingWindow  # noqa: E402

from config import load_config  # noqa: E402
from feed_health import FeedHealthTracker, FeedSignals, HealthThresholds  # noqa: E402


class SymbolMonitor:
    def __init__(self):
        self.msg_rate = RateCounter(window_seconds=10.0)
        self.alert_rate = RateCounter(window_seconds=30.0)
        self.latency_window = RollingWindow(maxlen=500)
        self.last_seen: float | None = None


class MonitoringService:
    def __init__(self, cfg):
        self.cfg = cfg
        self.log = configure_logging("monitoring", cfg.log_level)
        self.metrics = build_core_metrics()
        thresholds = HealthThresholds(
            healthy_min_msg_per_sec=cfg.healthy_min_msg_per_sec,
            degraded_min_msg_per_sec=cfg.degraded_min_msg_per_sec,
            stale_no_message_seconds=cfg.stale_no_message_seconds,
            offline_no_message_seconds=cfg.offline_no_message_seconds,
            p99_latency_alert_seconds=cfg.p99_latency_alert_seconds,
            error_rate_alert_threshold=cfg.error_rate_alert_threshold,
        )
        self.tracker = FeedHealthTracker(thresholds)
        self.dedup = AlertDeduplicator(cooldown_seconds=cfg.alert_cooldown_seconds)
        self.symbol_monitors: dict[str, SymbolMonitor] = {s: SymbolMonitor() for s in cfg.symbol_list}
        self._latest_signals: dict[str, dict] = {}
        self.pool = None
        self.producer = None
        self.market_consumer = None
        self.alerts_consumer = None
        self._start_time = time.time()

    def _sm(self, symbol: str) -> SymbolMonitor:
        return self.symbol_monitors.setdefault(symbol, SymbolMonitor())

    async def handle_market_message(self, raw: bytes) -> None:
        try:
            event = deserialize(raw)
        except Exception:
            return
        symbol = event.get("symbol")
        if not symbol:
            return
        now = time.time()
        sm = self._sm(symbol)
        sm.msg_rate.tick(now=now)
        sm.last_seen = now
        producer_ts = event.get("producer_timestamp")
        if isinstance(producer_ts, (int, float)):
            sm.latency_window.add(max(0.0, now - producer_ts))

    async def handle_alert_message(self, raw: bytes) -> None:
        try:
            alert = deserialize(raw)
        except Exception:
            return
        symbol = alert.get("symbol") or alert.get("feed")
        if not symbol:
            return
        self._sm(symbol).alert_rate.tick()

    async def _evaluate_loop(self):
        while True:
            await asyncio.sleep(self.cfg.evaluation_interval_seconds)
            now = time.time()
            for symbol, sm in self.symbol_monitors.items():
                seconds_since_last = (now - sm.last_seen) if sm.last_seen else (now - self._start_time)
                msg_rate = sm.msg_rate.rate(now=now)
                alert_rate = sm.alert_rate.rate(now=now)
                error_rate = min(1.0, alert_rate / msg_rate) if msg_rate > 0 else (1.0 if alert_rate > 0 else 0.0)
                p99 = sm.latency_window.percentile(99)

                signals = FeedSignals(
                    seconds_since_last_message=seconds_since_last,
                    messages_per_second=msg_rate,
                    p99_latency_seconds=p99,
                    error_rate=error_rate,
                )
                new_state, reason, changed = self.tracker.update(symbol, signals)

                self.metrics.feed_health.labels(feed=symbol).set(FEED_HEALTH_TO_INT[new_state])
                self.metrics.feed_messages_per_second.labels(feed=symbol).set(msg_rate)
                self._latest_signals[symbol] = {
                    "state": new_state,
                    "messages_per_second": msg_rate,
                    "p99_latency_seconds": p99,
                    "error_rate": error_rate,
                    "seconds_since_last_message": seconds_since_last,
                }

                if changed:
                    await self._on_state_change(symbol, new_state, reason)

                if p99 > self.cfg.p99_latency_alert_seconds:
                    await self._maybe_alert(
                        "HIGH", "LATENCY_THRESHOLD_EXCEEDED", symbol,
                        f"P99 latency for {symbol} is {p99:.3f}s (threshold {self.cfg.p99_latency_alert_seconds}s)",
                        {"p99_latency_seconds": p99},
                    )

                if new_state != "OFFLINE" and msg_rate < self.cfg.degraded_min_msg_per_sec:
                    await self._maybe_alert(
                        "MEDIUM", "THROUGHPUT_DEGRADED", symbol,
                        f"Throughput for {symbol} dropped to {msg_rate:.1f} msg/s",
                        {"messages_per_second": msg_rate},
                    )

                if error_rate > self.cfg.error_rate_alert_threshold:
                    await self._maybe_alert(
                        "MEDIUM", "ERROR_RATE_HIGH", symbol,
                        f"Error/anomaly rate for {symbol} is {error_rate:.1%}",
                        {"error_rate": error_rate},
                    )

    async def _on_state_change(self, symbol: str, new_state: str, reason: str):
        previous = self.tracker.previous_state(symbol)
        self.log.info("feed_state_transition", feed=symbol, previous=previous, new=new_state, reason=reason)
        try:
            await insert_feed_status_transition(self.pool, symbol, previous, new_state, reason)
        except Exception as exc:  # noqa: BLE001
            self.log.warning("feed_transition_db_insert_failed", error=str(exc))

        severity_map = {"OFFLINE": "CRITICAL", "STALE": "HIGH", "DEGRADED": "MEDIUM"}
        alert_type_map = {"OFFLINE": "FEED_OFFLINE", "STALE": "FEED_STALE", "DEGRADED": "FEED_DEGRADED"}

        if new_state == "HEALTHY":
            # Resolve any open feed-state alerts for this symbol now that
            # signals are nominal again, and reset cooldowns so a future
            # degradation alerts promptly instead of waiting out the window.
            for at in ("FEED_OFFLINE", "FEED_STALE", "FEED_DEGRADED"):
                key = make_dedup_key(at, symbol, symbol)
                try:
                    await resolve_alerts_by_dedup_key(self.pool, key)
                except Exception:  # noqa: BLE001
                    pass
                self.dedup.clear(key)
            return

        await self._maybe_alert(
            severity_map[new_state], alert_type_map[new_state], symbol,
            f"Feed {symbol} transitioned to {new_state}: {reason}",
            {"previous_state": previous, "new_state": new_state},
            source="FEED_HEALTH_MONITOR",
        )

    async def _maybe_alert(self, severity, alert_type, symbol, description, metrics, source="FEED_HEALTH_MONITOR"):
        dedup_key = make_dedup_key(alert_type, symbol, symbol)
        if not self.dedup.should_fire(dedup_key):
            return
        alert = build_alert(
            severity=severity, alert_type=alert_type, feed=symbol, description=description,
            detection_source=source, symbol=symbol, metrics=metrics,
        )
        await emit_alert(self.pool, self.producer, self.cfg.topic_alerts, alert, log=self.log)

    async def _active_alerts_poll_loop(self):
        while True:
            await asyncio.sleep(self.cfg.active_alerts_poll_interval_seconds)
            try:
                rows = await self.pool.fetch(
                    "SELECT severity, count(*) AS n FROM alerts WHERE status = 'ACTIVE' GROUP BY severity"
                )
                seen = set()
                for row in rows:
                    self.metrics.active_alerts.labels(severity=row["severity"]).set(row["n"])
                    seen.add(row["severity"])
                for sev in ("CRITICAL", "HIGH", "MEDIUM", "LOW"):
                    if sev not in seen:
                        self.metrics.active_alerts.labels(severity=sev).set(0)
            except Exception as exc:  # noqa: BLE001
                self.log.warning("active_alerts_poll_failed", error=str(exc))

    async def _aggregated_metrics_loop(self, interval_seconds: float = 30.0):
        while True:
            await asyncio.sleep(interval_seconds)
            window_start = time.time()
            for symbol, snapshot in self._latest_signals.items():
                try:
                    await upsert_aggregated_metrics(self.pool, symbol, window_start, snapshot)
                except Exception as exc:  # noqa: BLE001
                    self.log.warning("aggregated_metrics_write_failed", feed=symbol, error=str(exc))

    async def _consumer_lag_poll_loop(self):
        while True:
            await asyncio.sleep(5.0)
            for consumer, topic, group in (
                (self.market_consumer, self.cfg.topic_market_data, f"{self.cfg.consumer_group_suffix}-market"),
                (self.alerts_consumer, self.cfg.topic_alerts, f"{self.cfg.consumer_group_suffix}-alerts"),
            ):
                lag = await get_total_consumer_lag(consumer)
                self.metrics.kafka_consumer_lag.labels(topic=topic, group=group).set(lag)
                if lag > 5000:
                    await self._maybe_alert(
                        "MEDIUM", "CONSUMER_LAG_HIGH", topic,
                        f"Consumer lag for group {group} on {topic} is {lag}",
                        {"lag": lag, "group": group}, source="THROUGHPUT_MONITOR",
                    )

    async def run(self):
        start_http_server(self.cfg.metrics_port)
        self.log.info("metrics_server_started", port=self.cfg.metrics_port)

        self.pool = await make_pool(self.cfg.database_url)
        self.producer = await make_producer(self.cfg.kafka_bootstrap_servers)

        self.market_consumer = make_consumer(
            [self.cfg.topic_market_data], f"{self.cfg.consumer_group_suffix}-market", self.cfg.kafka_bootstrap_servers
        )
        self.alerts_consumer = make_consumer(
            [self.cfg.topic_alerts], f"{self.cfg.consumer_group_suffix}-alerts", self.cfg.kafka_bootstrap_servers
        )
        await self.market_consumer.start()
        await self.alerts_consumer.start()
        self.log.info("monitoring_started", symbols=list(self.symbol_monitors.keys()))

        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop_event.set)
            except NotImplementedError:
                pass

        tasks = [
            asyncio.create_task(run_concurrent_consumer(self.market_consumer, self.handle_market_message, concurrency=self.cfg.consumer_concurrency)),
            asyncio.create_task(run_concurrent_consumer(self.alerts_consumer, self.handle_alert_message, concurrency=2)),
            asyncio.create_task(self._evaluate_loop()),
            asyncio.create_task(self._active_alerts_poll_loop()),
            asyncio.create_task(self._consumer_lag_poll_loop()),
            asyncio.create_task(self._aggregated_metrics_loop()),
        ]

        await stop_event.wait()
        self.log.info("monitoring_shutting_down")
        for t in tasks:
            t.cancel()
        for t in tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass
        await self.market_consumer.stop()
        await self.alerts_consumer.stop()
        await self.producer.stop()
        await self.pool.close()
        self.log.info("monitoring_stopped")


async def main():
    cfg = load_config()
    service = MonitoringService(cfg)
    await service.run()


if __name__ == "__main__":
    asyncio.run(main())
