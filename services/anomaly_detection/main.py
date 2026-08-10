"""Anomaly detection service.

Consumes the market-data topic on its own consumer group (independent from
ingestion) and runs two complementary detectors per event:
  1. Statistical / rule-based: rolling z-score + pct-change threshold.
  2. Isolation Forest: unsupervised outlier scoring over an engineered
     feature vector, retrained periodically per symbol.

Both detectors' findings are persisted to the `anomalies` table and, when
they cross an alert-worthy bar, published as structured alerts.
"""
from __future__ import annotations

import asyncio
import signal
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "common"))

from prometheus_client import start_http_server  # noqa: E402

from pulseguard_common.alerting import AlertDeduplicator, build_alert, emit_alert  # noqa: E402
from pulseguard_common.db import insert_anomaly, make_pool  # noqa: E402
from pulseguard_common.kafka_utils import (  # noqa: E402
    deserialize,
    make_consumer,
    make_producer,
    periodic_lag_reporter,
    run_concurrent_consumer,
)
from pulseguard_common.logging_utils import configure_logging  # noqa: E402
from pulseguard_common.metrics import build_core_metrics  # noqa: E402
from pulseguard_common.models import new_id  # noqa: E402

from config import load_config  # noqa: E402
from features import FeatureTracker  # noqa: E402
from isolation_forest_detector import IsolationForestDetector  # noqa: E402
from statistical import StatisticalDetector  # noqa: E402


class AnomalyDetectionService:
    def __init__(self, cfg):
        self.cfg = cfg
        self.log = configure_logging("anomaly_detection", cfg.log_level)
        self.metrics = build_core_metrics()
        self.tracker = FeatureTracker(window_size=cfg.rolling_window_size)
        self.statistical = StatisticalDetector(cfg.zscore_threshold, cfg.pct_change_threshold)
        self.isolation_forests: dict[str, IsolationForestDetector] = {}
        self.dedup = AlertDeduplicator(cooldown_seconds=cfg.alert_cooldown_seconds)
        self.pool = None
        self.producer = None
        self.consumer = None

    def _isolation_forest(self, symbol: str) -> IsolationForestDetector:
        if symbol not in self.isolation_forests:
            self.isolation_forests[symbol] = IsolationForestDetector(
                contamination=self.cfg.isolation_forest_contamination,
                retrain_every_n=self.cfg.isolation_forest_retrain_every_n,
                min_train_samples=self.cfg.isolation_forest_min_train_samples,
            )
        return self.isolation_forests[symbol]

    async def handle_message(self, raw: bytes) -> None:
        start = time.perf_counter()
        try:
            event = deserialize(raw)
        except Exception:
            return  # malformed payloads are ingestion's dead-letter concern

        price = event.get("price")
        bid = event.get("bid")
        ask = event.get("ask")
        if not isinstance(price, (int, float)) or price <= 0:
            return
        if isinstance(bid, (int, float)) and isinstance(ask, (int, float)) and bid > ask:
            return

        symbol = event["symbol"]
        fv = self.tracker.observe(event)

        stat_result = self.statistical.evaluate(fv)
        is_if_anomaly, if_score = self._isolation_forest(symbol).observe_and_score(fv)

        if stat_result.is_anomaly:
            severity = "HIGH" if abs(stat_result.pct_change) > self.cfg.pct_change_threshold * 3 else "MEDIUM"
            await self._record_anomaly(
                symbol=symbol,
                method="statistical",
                severity=severity,
                score=stat_result.zscore,
                description=f"Statistical anomaly on {symbol}: {', '.join(stat_result.reasons)}",
                metrics={
                    "zscore": stat_result.zscore,
                    "pct_change": stat_result.pct_change,
                    "price": fv.price,
                    "rolling_mean_price": fv.rolling_mean_price,
                },
                alert_type="PRICE_ANOMALY" if abs(stat_result.pct_change) > self.cfg.pct_change_threshold else "STATISTICAL_ANOMALY",
            )

        if is_if_anomaly:
            await self._record_anomaly(
                symbol=symbol,
                method="isolation_forest",
                severity="MEDIUM",
                score=if_score,
                description=f"Isolation Forest flagged an outlier event on {symbol}",
                metrics={
                    "anomaly_score": if_score,
                    "price_return": fv.price_return,
                    "spread_pct": fv.spread_pct,
                    "quantity": fv.quantity,
                },
                alert_type="ISOLATION_FOREST_ANOMALY",
            )

        self.metrics.processing_latency_seconds.labels(stage="anomaly_detection").observe(
            time.perf_counter() - start
        )

    async def _record_anomaly(self, symbol, method, severity, score, description, metrics, alert_type):
        self.metrics.anomalies_total.labels(feed=symbol, method=method, severity=severity).inc()

        anomaly = {
            "anomaly_id": new_id("anom_"),
            "detected_at": time.time(),
            "symbol": symbol,
            "detection_method": method,
            "severity": severity,
            "anomaly_score": float(score),
            "description": description,
            "metrics": metrics,
            "event_id": None,
        }
        try:
            await insert_anomaly(self.pool, anomaly)
        except Exception as exc:  # noqa: BLE001
            self.log.warning("anomaly_db_insert_failed", error=str(exc))

        dedup_key = f"{alert_type}:{symbol}:{symbol}"
        if self.dedup.should_fire(dedup_key):
            alert = build_alert(
                severity=severity,
                alert_type=alert_type,
                feed=symbol,
                description=description,
                detection_source="STATISTICAL_ENGINE" if method == "statistical" else "ISOLATION_FOREST",
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
        self.log.info("anomaly_detection_started", topic=self.cfg.topic_market_data, group=self.cfg.consumer_group)

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
        self.log.info("anomaly_detection_shutting_down")
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
        self.log.info("anomaly_detection_stopped")


async def main():
    cfg = load_config()
    service = AnomalyDetectionService(cfg)
    await service.run()


if __name__ == "__main__":
    asyncio.run(main())
