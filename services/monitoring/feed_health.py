"""Feed health state machine.

Pure and dependency-free so it can be unit tested without Kafka/Postgres:
`evaluate()` takes a snapshot of measurable signals and returns a state.
The caller (main.py) is responsible for gathering those signals and for
persisting/alerting on transitions.
"""
from __future__ import annotations

from dataclasses import dataclass


HEALTHY = "HEALTHY"
DEGRADED = "DEGRADED"
STALE = "STALE"
OFFLINE = "OFFLINE"


@dataclass
class HealthThresholds:
    healthy_min_msg_per_sec: float = 50
    degraded_min_msg_per_sec: float = 5
    stale_no_message_seconds: float = 5
    offline_no_message_seconds: float = 15
    p99_latency_alert_seconds: float = 1.0
    error_rate_alert_threshold: float = 0.05


@dataclass
class FeedSignals:
    seconds_since_last_message: float
    messages_per_second: float
    p99_latency_seconds: float
    error_rate: float  # fraction of recent messages that were invalid/anomalous


def evaluate(signals: FeedSignals, thresholds: HealthThresholds) -> tuple[str, str]:
    """Returns (state, reason)."""
    if signals.seconds_since_last_message >= thresholds.offline_no_message_seconds:
        return OFFLINE, f"no message received for {signals.seconds_since_last_message:.1f}s"

    if signals.seconds_since_last_message >= thresholds.stale_no_message_seconds:
        return STALE, f"no message received for {signals.seconds_since_last_message:.1f}s"

    if signals.messages_per_second < thresholds.degraded_min_msg_per_sec:
        return DEGRADED, f"throughput {signals.messages_per_second:.1f} msg/s below degraded floor"

    if signals.error_rate > thresholds.error_rate_alert_threshold:
        return DEGRADED, f"error rate {signals.error_rate:.1%} exceeds threshold"

    if signals.p99_latency_seconds > thresholds.p99_latency_alert_seconds:
        return DEGRADED, f"p99 latency {signals.p99_latency_seconds:.3f}s exceeds threshold"

    if signals.messages_per_second < thresholds.healthy_min_msg_per_sec:
        return DEGRADED, f"throughput {signals.messages_per_second:.1f} msg/s below healthy floor"

    return HEALTHY, "all signals nominal"


class FeedHealthTracker:
    """Keeps last-known state per feed so callers can detect transitions."""

    def __init__(self, thresholds: HealthThresholds):
        self.thresholds = thresholds
        self._state: dict[str, str] = {}

    def update(self, feed: str, signals: FeedSignals) -> tuple[str, str, bool]:
        """Returns (new_state, reason, changed)."""
        new_state, reason = evaluate(signals, self.thresholds)
        previous = self._state.get(feed)
        changed = previous != new_state
        self._state[feed] = new_state
        return new_state, reason, changed

    def previous_state(self, feed: str) -> str | None:
        return self._state.get(feed)
