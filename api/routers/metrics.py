from __future__ import annotations

from fastapi import APIRouter, Request

import db_queries
from schemas import MetricsSnapshot

router = APIRouter(prefix="/api/metrics", tags=["metrics"])


@router.get("", response_model=MetricsSnapshot)
async def get_metrics(request: Request):
    pool = request.app.state.pool
    prom = request.app.state.prom

    msg_per_sec = await prom.scalar("sum(rate(pulseguard_messages_received_total[1m]))")
    p50 = await prom.scalar(
        "histogram_quantile(0.50, sum(rate(pulseguard_market_data_latency_seconds_bucket[1m])) by (le))"
    )
    p95 = await prom.scalar(
        "histogram_quantile(0.95, sum(rate(pulseguard_market_data_latency_seconds_bucket[1m])) by (le))"
    )
    p99 = await prom.scalar(
        "histogram_quantile(0.99, sum(rate(pulseguard_market_data_latency_seconds_bucket[1m])) by (le))"
    )
    # NOT sum(): market-data has THREE independent consumer groups reading
    # it (ingestion, anomaly-detection, monitoring's "-market" group) plus
    # monitoring's "-alerts" group on a separate topic. Each group's lag is
    # already a complete measure of that group's own backlog against the
    # topic it reads — summing separate groups together doesn't produce a
    # meaningful "total backlog", it just multiplies the displayed number
    # by however many groups happen to be behind at once. max() surfaces
    # "how far behind is the worst-lagging consumer group right now",
    # which is what this single scalar tile is meant to convey (Grafana's
    # "Kafka consumer lag" panel still plots every group as its own
    # series, unsummed, for the full breakdown).
    lag = await prom.scalar("max(pulseguard_kafka_consumer_lag)")
    rejected_rate = await prom.scalar("sum(rate(pulseguard_messages_rejected_total[1m]))")
    anomalies_5m = await db_queries.anomaly_count_since(pool, seconds=300)

    return MetricsSnapshot(
        messages_per_second_total=msg_per_sec,
        p50_latency_seconds=p50,
        p95_latency_seconds=p95,
        p99_latency_seconds=p99,
        active_anomalies_last_5m=anomalies_5m,
        consumer_lag_total=lag,
        rejected_rate_per_second=rejected_rate,
    )
