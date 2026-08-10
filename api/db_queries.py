"""Read-only query helpers for the API layer. Kept separate from
pulseguard_common.db (which is write-path focused for the detection
services) since the API's query shapes are driven by dashboard needs.
"""
from __future__ import annotations

from typing import Optional

import asyncpg


async def latest_feed_states(pool: asyncpg.Pool) -> dict[str, dict]:
    rows = await pool.fetch(
        """
        SELECT DISTINCT ON (feed) feed, new_state, reason, transitioned_at
        FROM feed_status_transitions
        ORDER BY feed, transitioned_at DESC
        """
    )
    return {
        r["feed"]: {"state": r["new_state"], "reason": r["reason"], "since": r["transitioned_at"]}
        for r in rows
    }


async def feed_transition_history(pool: asyncpg.Pool, feed: str, limit: int = 50) -> list[dict]:
    rows = await pool.fetch(
        """
        SELECT feed, previous_state, new_state, reason, transitioned_at
        FROM feed_status_transitions
        WHERE feed = $1
        ORDER BY transitioned_at DESC
        LIMIT $2
        """,
        feed,
        limit,
    )
    return [dict(r) for r in rows]


async def list_alerts(
    pool: asyncpg.Pool,
    status: Optional[str] = None,
    severity: Optional[str] = None,
    feed: Optional[str] = None,
    limit: int = 100,
) -> list[dict]:
    conditions = []
    params: list = []
    if status:
        params.append(status)
        conditions.append(f"status = ${len(params)}")
    if severity:
        params.append(severity)
        conditions.append(f"severity = ${len(params)}")
    if feed:
        params.append(feed)
        conditions.append(f"feed = ${len(params)}")
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    params.append(limit)
    rows = await pool.fetch(
        f"""
        SELECT alert_id, created_at, resolved_at, severity, alert_type, feed, symbol,
               description, metrics, detection_source, status
        FROM alerts
        {where}
        ORDER BY created_at DESC
        LIMIT ${len(params)}
        """,
        *params,
    )
    return [dict(r) for r in rows]


async def list_anomalies(
    pool: asyncpg.Pool,
    symbol: Optional[str] = None,
    method: Optional[str] = None,
    limit: int = 100,
) -> list[dict]:
    conditions = []
    params: list = []
    if symbol:
        params.append(symbol)
        conditions.append(f"symbol = ${len(params)}")
    if method:
        params.append(method)
        conditions.append(f"detection_method = ${len(params)}")
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    params.append(limit)
    rows = await pool.fetch(
        f"""
        SELECT anomaly_id, detected_at, symbol, detection_method, severity,
               anomaly_score, description, metrics, event_id
        FROM anomalies
        {where}
        ORDER BY detected_at DESC
        LIMIT ${len(params)}
        """,
        *params,
    )
    return [dict(r) for r in rows]


async def active_alert_counts(pool: asyncpg.Pool) -> dict[str, int]:
    rows = await pool.fetch("SELECT severity, count(*) AS n FROM alerts WHERE status = 'ACTIVE' GROUP BY severity")
    return {r["severity"]: r["n"] for r in rows}


async def anomaly_count_since(pool: asyncpg.Pool, seconds: int = 300) -> int:
    row = await pool.fetchrow(
        "SELECT count(*) AS n FROM anomalies WHERE detected_at >= now() - ($1 || ' seconds')::interval",
        str(seconds),
    )
    return row["n"] if row else 0
