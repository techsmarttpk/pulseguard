"""Unit tests for the shared in-memory rolling statistics (latency
percentiles, throughput rate counting) used by ingestion and monitoring.
"""
import time

from pulseguard_common.stats import RateCounter, RollingWindow


def test_rolling_window_percentiles():
    w = RollingWindow(maxlen=1000)
    for i in range(1, 101):
        w.add(i / 1000.0)  # 0.001 .. 0.100 seconds
    snap = w.snapshot()
    assert snap["count"] == 100
    assert 0.049 < snap["p50"] < 0.052
    assert snap["p99"] > snap["p95"] > snap["p50"]
    assert snap["max"] == 0.1


def test_rolling_window_respects_maxlen():
    w = RollingWindow(maxlen=10)
    for i in range(100):
        w.add(i)
    assert len(w) == 10


def test_rolling_window_zscore():
    w = RollingWindow(maxlen=100)
    for _ in range(50):
        w.add(100.0)
    # A constant series has zero std; z-score must not divide by zero.
    assert w.zscore(150.0) == 0.0

    w2 = RollingWindow(maxlen=100)
    for v in [100, 101, 99, 100, 102, 98, 100, 101, 99, 100]:
        w2.add(v)
    z = w2.zscore(200.0)
    assert z > 5  # a huge jump should score as a large outlier


def test_rate_counter_basic():
    rc = RateCounter(window_seconds=10.0)
    now = time.time()
    for i in range(50):
        rc.tick(now=now)
    assert abs(rc.rate(now=now) - 5.0) < 0.01  # 50 events / 10s window


def test_rate_counter_evicts_old_events():
    rc = RateCounter(window_seconds=1.0)
    t0 = 1000.0
    rc.tick(n=10, now=t0)
    assert rc.rate(now=t0) == 10.0
    # Advance well past the window; old events should be evicted.
    assert rc.rate(now=t0 + 5.0) == 0.0


def test_rate_counter_seconds_since_last():
    rc = RateCounter(window_seconds=10.0)
    t0 = 1000.0
    rc.tick(now=t0)
    assert rc.seconds_since_last(now=t0 + 3.0) == 3.0


def test_rate_counter_no_events_is_infinite_gap():
    rc = RateCounter(window_seconds=10.0)
    assert rc.seconds_since_last(now=1000.0) == float("inf")
