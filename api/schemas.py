from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel


class FeedSummary(BaseModel):
    feed: str
    state: str
    reason: Optional[str] = None
    since: Optional[datetime] = None
    messages_per_second: float = 0.0
    p95_latency_seconds: float = 0.0
    p99_latency_seconds: float = 0.0


class FeedTransition(BaseModel):
    feed: str
    previous_state: Optional[str]
    new_state: str
    reason: Optional[str]
    transitioned_at: datetime


class FeedDetail(BaseModel):
    feed: str
    state: str
    reason: Optional[str] = None
    messages_per_second: float = 0.0
    p50_latency_seconds: float = 0.0
    p95_latency_seconds: float = 0.0
    p99_latency_seconds: float = 0.0
    consumer_lag: float = 0.0
    recent_transitions: list[FeedTransition] = []


class Alert(BaseModel):
    alert_id: str
    created_at: datetime
    resolved_at: Optional[datetime] = None
    severity: str
    alert_type: str
    feed: str
    symbol: Optional[str] = None
    description: str
    metrics: dict[str, Any] = {}
    detection_source: str
    status: str


class Anomaly(BaseModel):
    anomaly_id: str
    detected_at: datetime
    symbol: str
    detection_method: str
    severity: str
    anomaly_score: float
    description: str
    metrics: dict[str, Any] = {}
    event_id: Optional[str] = None


class MetricsSnapshot(BaseModel):
    messages_per_second_total: float
    p50_latency_seconds: float
    p95_latency_seconds: float
    p99_latency_seconds: float
    active_anomalies_last_5m: int
    consumer_lag_total: float
    rejected_rate_per_second: float


class SystemStatus(BaseModel):
    overall_state: str
    feeds_healthy: int
    feeds_degraded: int
    feeds_stale: int
    feeds_offline: int
    active_alerts_by_severity: dict[str, int]
