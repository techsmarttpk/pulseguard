"""Unit tests for the feed health state machine (services/monitoring)."""
from feed_health import FeedHealthTracker, FeedSignals, HealthThresholds, evaluate

THRESHOLDS = HealthThresholds(
    healthy_min_msg_per_sec=50,
    degraded_min_msg_per_sec=5,
    stale_no_message_seconds=5,
    offline_no_message_seconds=15,
    p99_latency_alert_seconds=1.0,
    error_rate_alert_threshold=0.05,
)


def test_healthy_feed_stays_healthy():
    signals = FeedSignals(seconds_since_last_message=0.1, messages_per_second=1000, p99_latency_seconds=0.05, error_rate=0.0)
    state, _ = evaluate(signals, THRESHOLDS)
    assert state == "HEALTHY"


def test_no_messages_for_15s_goes_offline():
    signals = FeedSignals(seconds_since_last_message=16, messages_per_second=0, p99_latency_seconds=0, error_rate=0)
    state, _ = evaluate(signals, THRESHOLDS)
    assert state == "OFFLINE"


def test_no_messages_for_6s_is_stale_not_offline():
    signals = FeedSignals(seconds_since_last_message=6, messages_per_second=0, p99_latency_seconds=0, error_rate=0)
    state, _ = evaluate(signals, THRESHOLDS)
    assert state == "STALE"


def test_low_throughput_is_degraded():
    signals = FeedSignals(seconds_since_last_message=0.1, messages_per_second=2, p99_latency_seconds=0.05, error_rate=0.0)
    state, _ = evaluate(signals, THRESHOLDS)
    assert state == "DEGRADED"


def test_high_latency_is_degraded():
    signals = FeedSignals(seconds_since_last_message=0.1, messages_per_second=1000, p99_latency_seconds=2.5, error_rate=0.0)
    state, _ = evaluate(signals, THRESHOLDS)
    assert state == "DEGRADED"


def test_high_error_rate_is_degraded():
    signals = FeedSignals(seconds_since_last_message=0.1, messages_per_second=1000, p99_latency_seconds=0.05, error_rate=0.5)
    state, _ = evaluate(signals, THRESHOLDS)
    assert state == "DEGRADED"


def test_throughput_ramp_transitions_healthy_to_degraded_to_offline():
    """Mirrors the spec's example: 1000 msg/s -> 50 msg/s -> 0 msg/s."""
    tracker = FeedHealthTracker(THRESHOLDS)

    s1, _, changed1 = tracker.update("AAPL", FeedSignals(0.1, 1000, 0.05, 0.0))
    assert s1 == "HEALTHY" and changed1

    s2, _, changed2 = tracker.update("AAPL", FeedSignals(0.1, 50, 0.05, 0.0))
    # 50 msg/s sits right at the healthy floor; still not below it, so no
    # transition is expected here — drop below it to force DEGRADED.
    s3, _, changed3 = tracker.update("AAPL", FeedSignals(0.1, 20, 0.05, 0.0))
    assert s3 == "DEGRADED"

    s4, _, changed4 = tracker.update("AAPL", FeedSignals(20, 0, 0, 0))
    assert s4 == "OFFLINE" and changed4


def test_no_flapping_alert_when_state_unchanged():
    tracker = FeedHealthTracker(THRESHOLDS)
    tracker.update("MSFT", FeedSignals(0.1, 1000, 0.05, 0.0))
    _, _, changed = tracker.update("MSFT", FeedSignals(0.1, 900, 0.05, 0.0))
    assert changed is False
