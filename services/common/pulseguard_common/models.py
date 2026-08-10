"""Shared data models / wire schemas for PulseGuard.

These mirror the Kafka message payloads (JSON) exchanged between the
simulator, ingestion, anomaly-detection and monitoring services, plus the
rows persisted to PostgreSQL and returned by the FastAPI layer.
"""
from __future__ import annotations

import enum
import time
import uuid
from dataclasses import dataclass, field, asdict
from typing import Any, Optional


class EventType(str, enum.Enum):
    TRADE = "TRADE"
    QUOTE = "QUOTE"
    BID_ASK = "BID_ASK"


class FeedHealthState(str, enum.Enum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    STALE = "STALE"
    OFFLINE = "OFFLINE"


class AlertSeverity(str, enum.Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class AlertType(str, enum.Enum):
    FEED_OFFLINE = "FEED_OFFLINE"
    FEED_DEGRADED = "FEED_DEGRADED"
    FEED_STALE = "FEED_STALE"
    LATENCY_THRESHOLD_EXCEEDED = "LATENCY_THRESHOLD_EXCEEDED"
    PRICE_ANOMALY = "PRICE_ANOMALY"
    STATISTICAL_ANOMALY = "STATISTICAL_ANOMALY"
    ISOLATION_FOREST_ANOMALY = "ISOLATION_FOREST_ANOMALY"
    THROUGHPUT_DEGRADED = "THROUGHPUT_DEGRADED"
    SEQUENCE_GAP = "SEQUENCE_GAP"
    DUPLICATE_EVENT = "DUPLICATE_EVENT"
    INVALID_EVENT = "INVALID_EVENT"
    CONSUMER_LAG_HIGH = "CONSUMER_LAG_HIGH"
    ERROR_RATE_HIGH = "ERROR_RATE_HIGH"


class DetectionSource(str, enum.Enum):
    VALIDATION_ENGINE = "VALIDATION_ENGINE"
    STATISTICAL_ENGINE = "STATISTICAL_ENGINE"
    ISOLATION_FOREST = "ISOLATION_FOREST"
    FEED_HEALTH_MONITOR = "FEED_HEALTH_MONITOR"
    LATENCY_MONITOR = "LATENCY_MONITOR"
    THROUGHPUT_MONITOR = "THROUGHPUT_MONITOR"


def new_id(prefix: str = "") -> str:
    return f"{prefix}{uuid.uuid4().hex}"


@dataclass
class MarketEvent:
    """A single simulated market-data message (trade / quote / bid-ask)."""

    event_id: str
    sequence_number: int
    symbol: str
    exchange: str
    event_type: str
    price: float
    quantity: float
    bid: float
    ask: float
    producer_timestamp: float  # unix epoch seconds, set by the simulator
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict) -> "MarketEvent":
        return MarketEvent(
            event_id=d["event_id"],
            sequence_number=d["sequence_number"],
            symbol=d["symbol"],
            exchange=d["exchange"],
            event_type=d["event_type"],
            price=d["price"],
            quantity=d["quantity"],
            bid=d["bid"],
            ask=d["ask"],
            producer_timestamp=d["producer_timestamp"],
            metadata=d.get("metadata") or {},
        )


@dataclass
class ValidationResult:
    is_valid: bool
    reasons: list[str] = field(default_factory=list)
    severity: Optional[str] = None
    alert_type: Optional[str] = None


@dataclass
class AnomalyRecord:
    anomaly_id: str
    detected_at: float
    symbol: str
    detection_method: str  # "statistical" | "isolation_forest" | "validation"
    severity: str
    anomaly_score: float
    description: str
    metrics: dict = field(default_factory=dict)
    event_id: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class Alert:
    alert_id: str
    created_at: float
    severity: str
    alert_type: str
    feed: str
    symbol: Optional[str]
    description: str
    metrics: dict
    detection_source: str
    dedup_key: str
    status: str = "ACTIVE"  # ACTIVE | RESOLVED

    def to_dict(self) -> dict:
        return asdict(self)


def now_ts() -> float:
    return time.time()
