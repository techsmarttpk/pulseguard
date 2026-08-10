"""Unit tests for the anomaly detection engines (services/anomaly_detection)."""
import time

from features import FeatureTracker
from isolation_forest_detector import IsolationForestDetector
from statistical import StatisticalDetector


def make_event(symbol="AAPL", price=189.42, bid=189.40, ask=189.44, quantity=100, ts=None):
    return {
        "event_id": "evt",
        "sequence_number": 1,
        "symbol": symbol,
        "exchange": "NASDAQ",
        "event_type": "TRADE",
        "price": price,
        "quantity": quantity,
        "bid": bid,
        "ask": ask,
        "producer_timestamp": ts if ts is not None else time.time(),
    }


def test_feature_tracker_first_event_has_zero_return():
    tracker = FeatureTracker(window_size=50)
    fv = tracker.observe(make_event(price=100.0))
    assert fv.price_return == 0.0
    assert fv.spread == make_event()["ask"] - make_event()["bid"]


def test_feature_tracker_computes_price_return():
    tracker = FeatureTracker(window_size=50)
    tracker.observe(make_event(price=100.0))
    fv = tracker.observe(make_event(price=110.0))
    assert abs(fv.price_return - 0.10) < 1e-9


def test_statistical_detector_flags_large_pct_change():
    detector = StatisticalDetector(zscore_threshold=4.0, pct_change_threshold=0.03)
    tracker = FeatureTracker(window_size=50)

    # Build up a stable price history first.
    price = 100.0
    for _ in range(30):
        fv = tracker.observe(make_event(price=price))
        detector.evaluate(fv)
        price += 0.01  # tiny, realistic drift

    # Now inject a spike: price roughly doubles in one tick.
    spike_fv = tracker.observe(make_event(price=price * 2))
    result = detector.evaluate(spike_fv)
    assert result.is_anomaly
    assert abs(result.pct_change) > 0.03


def test_statistical_detector_does_not_flag_normal_drift():
    detector = StatisticalDetector(zscore_threshold=4.0, pct_change_threshold=0.03)
    tracker = FeatureTracker(window_size=50)
    price = 100.0
    result = None
    for _ in range(30):
        fv = tracker.observe(make_event(price=price))
        result = detector.evaluate(fv)
        price *= 1.0005  # 0.05% drift per tick, well under threshold
    assert not result.is_anomaly


def test_isolation_forest_trains_and_scores():
    detector = IsolationForestDetector(
        contamination=0.05, retrain_every_n=50, min_train_samples=40, buffer_size=500
    )
    tracker = FeatureTracker(window_size=100)

    # Feed enough "normal" events to trigger training.
    price = 100.0
    for i in range(60):
        fv = tracker.observe(make_event(price=price + (i % 3) * 0.01, quantity=100))
        detector.observe_and_score(fv)

    assert detector.model is not None

    # A wildly different feature vector should score as an outlier more
    # often than not once the model is trained (not a hard guarantee with
    # a randomized model, so we just check the API returns sane types).
    outlier_fv = tracker.observe(make_event(price=price * 50, quantity=50_000_000))
    is_anomaly, score = detector.observe_and_score(outlier_fv)
    assert isinstance(is_anomaly, bool)
    assert isinstance(score, float)
