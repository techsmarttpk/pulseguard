"""PostgreSQL access layer (asyncpg) shared by services and the API.

PulseGuard deliberately does NOT persist every tick. Only alerts, anomalies,
feed-status transitions and periodic aggregated metrics are written here —
see database/init.sql for the schema and README.md "Storage strategy" for
the reasoning.
"""
from __future__ import annotations

import json
import os
from typing import Any, Optional

import asyncpg

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://pulseguard:pulseguard_dev_password@localhost:5432/pulseguard",
)


async def _init_connection(conn: asyncpg.Connection) -> None:
    # asyncpg returns jsonb columns as raw text by default; register a codec
    # so every caller (services and the API) gets back a real dict/list for
    # the `metrics` column instead of having to json.loads() it themselves
    # everywhere.
    await conn.set_type_codec(
        "jsonb",
        encoder=lambda v: v if isinstance(v, str) else json.dumps(v),
        decoder=json.loads,
        schema="pg_catalog",
        format="text",
    )


async def make_pool(dsn: str = DATABASE_URL, min_size: int = 2, max_size: int = 10) -> asyncpg.Pool:
    return await asyncpg.create_pool(dsn=dsn, min_size=min_size, max_size=max_size, init=_init_connection)


async def insert_alert(pool: asyncpg.Pool, alert: dict) -> None:
    await pool.execute(
        """
        INSERT INTO alerts (
            alert_id, created_at, severity, alert_type, feed, symbol,
            description, metrics, detection_source, dedup_key, status
        ) VALUES ($1, to_timestamp($2), $3, $4, $5, $6, $7, $8::jsonb, $9, $10, $11)
        ON CONFLICT (alert_id) DO NOTHING
        """,
        alert["alert_id"],
        alert["created_at"],
        alert["severity"],
        alert["alert_type"],
        alert["feed"],
        alert.get("symbol"),
        alert["description"],
        _to_json(alert.get("metrics") or {}),
        alert["detection_source"],
        alert["dedup_key"],
        alert.get("status", "ACTIVE"),
    )


async def resolve_alerts_by_dedup_key(pool: asyncpg.Pool, dedup_key: str) -> None:
    await pool.execute(
        "UPDATE alerts SET status = 'RESOLVED', resolved_at = now() "
        "WHERE dedup_key = $1 AND status = 'ACTIVE'",
        dedup_key,
    )


async def insert_anomaly(pool: asyncpg.Pool, anomaly: dict) -> None:
    await pool.execute(
        """
        INSERT INTO anomalies (
            anomaly_id, detected_at, symbol, detection_method, severity,
            anomaly_score, description, metrics, event_id
        ) VALUES ($1, to_timestamp($2), $3, $4, $5, $6, $7, $8::jsonb, $9)
        ON CONFLICT (anomaly_id) DO NOTHING
        """,
        anomaly["anomaly_id"],
        anomaly["detected_at"],
        anomaly["symbol"],
        anomaly["detection_method"],
        anomaly["severity"],
        anomaly["anomaly_score"],
        anomaly["description"],
        _to_json(anomaly.get("metrics") or {}),
        anomaly.get("event_id"),
    )


async def insert_feed_status_transition(
    pool: asyncpg.Pool, feed: str, previous_state: Optional[str], new_state: str, reason: str
) -> None:
    await pool.execute(
        """
        INSERT INTO feed_status_transitions (feed, previous_state, new_state, reason, transitioned_at)
        VALUES ($1, $2, $3, $4, now())
        """,
        feed,
        previous_state,
        new_state,
        reason,
    )


async def upsert_aggregated_metrics(pool: asyncpg.Pool, feed: str, window_start: float, metrics: dict) -> None:
    await pool.execute(
        """
        INSERT INTO aggregated_metrics (feed, window_start, metrics)
        VALUES ($1, to_timestamp($2), $3::jsonb)
        ON CONFLICT (feed, window_start) DO UPDATE SET metrics = EXCLUDED.metrics
        """,
        feed,
        window_start,
        _to_json(metrics),
    )


def _to_json(value: Any) -> str:
    import orjson

    return orjson.dumps(value).decode()
