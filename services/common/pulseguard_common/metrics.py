"""Prometheus metric definitions shared across services.

Each service process has its own default CollectorRegistry, so it is safe
for every service to call `build_core_metrics()` once at startup.
"""
from __future__ import annotations

from dataclasses import dataclass

from prometheus_client import Counter, Gauge, Histogram


@dataclass
class CoreMetrics:
    messages_received_total: Counter
    messages_processed_total: Counter
    messages_rejected_total: Counter
    duplicate_events_total: Counter
    anomalies_total: Counter
    processing_latency_seconds: Histogram
    market_data_latency_seconds: Histogram
    feed_messages_per_second: Gauge
    feed_health: Gauge
    kafka_consumer_lag: Gauge
    active_alerts: Gauge


def build_core_metrics() -> CoreMetrics:
    return CoreMetrics(
        messages_received_total=Counter(
            "pulseguard_messages_received_total",
            "Total market-data messages received from Kafka",
            ["feed"],
        ),
        messages_processed_total=Counter(
            "pulseguard_messages_processed_total",
            "Total market-data messages successfully processed",
            ["feed"],
        ),
        messages_rejected_total=Counter(
            "pulseguard_messages_rejected_total",
            "Total market-data messages rejected by validation",
            ["feed", "reason"],
        ),
        duplicate_events_total=Counter(
            "pulseguard_duplicate_events_total",
            "Total duplicate event_ids detected",
            ["feed"],
        ),
        anomalies_total=Counter(
            "pulseguard_anomalies_total",
            "Total anomalies detected",
            ["feed", "method", "severity"],
        ),
        processing_latency_seconds=Histogram(
            "pulseguard_processing_latency_seconds",
            "Time spent processing a single message inside a service",
            ["stage"],
            buckets=(0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5),
        ),
        market_data_latency_seconds=Histogram(
            "pulseguard_market_data_latency_seconds",
            "End-to-end latency from producer_timestamp to ingestion receive time",
            ["feed"],
            buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
        ),
        feed_messages_per_second=Gauge(
            "pulseguard_feed_messages_per_second",
            "Rolling messages/sec observed for a feed",
            ["feed"],
        ),
        feed_health=Gauge(
            "pulseguard_feed_health",
            "Feed health state as an integer: 3=HEALTHY 2=DEGRADED 1=STALE 0=OFFLINE",
            ["feed"],
        ),
        kafka_consumer_lag=Gauge(
            "pulseguard_kafka_consumer_lag",
            "Approximate consumer lag (messages) per topic/partition group",
            ["topic", "group"],
        ),
        active_alerts=Gauge(
            "pulseguard_active_alerts",
            "Number of currently active (non-resolved) alerts",
            ["severity"],
        ),
    )


FEED_HEALTH_TO_INT = {"HEALTHY": 3, "DEGRADED": 2, "STALE": 1, "OFFLINE": 0}
