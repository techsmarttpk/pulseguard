"""Shared alert construction, deduplication/cooldown, and persistence+publish
helper. Every detection service (ingestion validation, anomaly detection,
feed-health monitoring) uses this so alert semantics stay consistent instead
of three services inventing their own alert shapes.
"""
from __future__ import annotations

import time
from typing import Optional

import asyncpg
from aiokafka import AIOKafkaProducer

from . import db as db_module
from .kafka_utils import send_with_retry
from .models import Alert, new_id


class AlertDeduplicator:
    """Suppresses re-firing the same (alert_type, feed, symbol) alert while
    the underlying condition is still active, so a continuing incident (e.g.
    a feed stuck OFFLINE) produces one alert instead of one per tick."""

    def __init__(self, cooldown_seconds: float = 60.0):
        self.cooldown_seconds = cooldown_seconds
        self._last_fired: dict[str, float] = {}

    def should_fire(self, dedup_key: str, now: Optional[float] = None) -> bool:
        now = now if now is not None else time.time()
        last = self._last_fired.get(dedup_key)
        if last is None or (now - last) >= self.cooldown_seconds:
            self._last_fired[dedup_key] = now
            return True
        return False

    def clear(self, dedup_key: str) -> None:
        self._last_fired.pop(dedup_key, None)


def make_dedup_key(alert_type: str, feed: str, symbol: Optional[str]) -> str:
    return f"{alert_type}:{feed}:{symbol or '-'}"


def build_alert(
    severity: str,
    alert_type: str,
    feed: str,
    description: str,
    detection_source: str,
    symbol: Optional[str] = None,
    metrics: Optional[dict] = None,
) -> dict:
    dedup_key = make_dedup_key(alert_type, feed, symbol)
    return Alert(
        alert_id=new_id("alrt_"),
        created_at=time.time(),
        severity=severity,
        alert_type=alert_type,
        feed=feed,
        symbol=symbol,
        description=description,
        metrics=metrics or {},
        detection_source=detection_source,
        dedup_key=dedup_key,
    ).to_dict()


async def emit_alert(
    pool: Optional[asyncpg.Pool],
    producer: Optional[AIOKafkaProducer],
    alerts_topic: str,
    alert: dict,
    log=None,
) -> None:
    """Persist an alert to Postgres (source of truth for the API) and
    publish it to the alerts topic (for real-time fan-out / future
    consumers). Both are best-effort: a DB or Kafka hiccup must never take
    down a detection service."""
    if pool is not None:
        try:
            await db_module.insert_alert(pool, alert)
        except Exception as exc:  # noqa: BLE001
            if log:
                log.warning("alert_db_insert_failed", error=str(exc))
    if producer is not None:
        try:
            await send_with_retry(producer, alerts_topic, alert)
        except Exception as exc:  # noqa: BLE001
            if log:
                log.warning("alert_publish_failed", error=str(exc))
