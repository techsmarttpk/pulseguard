from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

import db_queries
from schemas import FeedDetail, FeedSummary, FeedTransition

router = APIRouter(prefix="/api/feeds", tags=["feeds"])


@router.get("", response_model=list[FeedSummary])
async def list_feeds(request: Request):
    pool = request.app.state.pool
    prom = request.app.state.prom
    symbols = request.app.state.cfg.symbol_list

    states = await db_queries.latest_feed_states(pool)
    rates = await prom.by_label("pulseguard_feed_messages_per_second", "feed")
    p95_map = await prom.by_label(
        "histogram_quantile(0.95, sum(rate(pulseguard_market_data_latency_seconds_bucket[1m])) by (le, feed))",
        "feed",
    )
    p99_map = await prom.by_label(
        "histogram_quantile(0.99, sum(rate(pulseguard_market_data_latency_seconds_bucket[1m])) by (le, feed))",
        "feed",
    )

    out = []
    for symbol in symbols:
        state_info = states.get(symbol, {"state": "OFFLINE", "reason": "no data yet", "since": None})
        out.append(
            FeedSummary(
                feed=symbol,
                state=state_info["state"],
                reason=state_info.get("reason"),
                since=state_info.get("since"),
                messages_per_second=rates.get(symbol, 0.0),
                p95_latency_seconds=p95_map.get(symbol, 0.0),
                p99_latency_seconds=p99_map.get(symbol, 0.0),
            )
        )
    return out


@router.get("/{feed_id}", response_model=FeedDetail)
async def get_feed(feed_id: str, request: Request):
    pool = request.app.state.pool
    prom = request.app.state.prom

    if feed_id not in request.app.state.cfg.symbol_list:
        raise HTTPException(status_code=404, detail="Unknown feed")

    states = await db_queries.latest_feed_states(pool)
    state_info = states.get(feed_id, {"state": "OFFLINE", "reason": "no data yet"})
    history_rows = await db_queries.feed_transition_history(pool, feed_id, limit=50)

    rate = await prom.scalar(f'pulseguard_feed_messages_per_second{{feed="{feed_id}"}}')
    p50 = await prom.scalar(
        f'histogram_quantile(0.50, sum(rate(pulseguard_market_data_latency_seconds_bucket{{feed="{feed_id}"}}[1m])) by (le))'
    )
    p95 = await prom.scalar(
        f'histogram_quantile(0.95, sum(rate(pulseguard_market_data_latency_seconds_bucket{{feed="{feed_id}"}}[1m])) by (le))'
    )
    p99 = await prom.scalar(
        f'histogram_quantile(0.99, sum(rate(pulseguard_market_data_latency_seconds_bucket{{feed="{feed_id}"}}[1m])) by (le))'
    )
    lag = await prom.scalar(f'sum(pulseguard_kafka_consumer_lag{{topic="market-data"}})')

    return FeedDetail(
        feed=feed_id,
        state=state_info["state"],
        reason=state_info.get("reason"),
        messages_per_second=rate,
        p50_latency_seconds=p50,
        p95_latency_seconds=p95,
        p99_latency_seconds=p99,
        consumer_lag=lag,
        recent_transitions=[FeedTransition(**row) for row in history_rows],
    )
