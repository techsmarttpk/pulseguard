"""Lightweight in-memory rolling statistics used for latency percentiles,
throughput, and z-score anomaly detection. Deliberately avoids any DB
round-trips — everything here is O(1) amortized append with periodic O(n)
recompute over a bounded window, which comfortably keeps up at tens of
thousands of messages/sec per process.
"""
from __future__ import annotations

import time
from collections import deque
from typing import Deque, Optional

import numpy as np


class RollingWindow:
    """Fixed-capacity numeric window with mean/std/percentile helpers."""

    def __init__(self, maxlen: int = 500):
        self.maxlen = maxlen
        self._data: Deque[float] = deque(maxlen=maxlen)

    def add(self, value: float) -> None:
        self._data.append(value)

    def __len__(self) -> int:
        return len(self._data)

    @property
    def mean(self) -> float:
        return float(np.mean(self._data)) if self._data else 0.0

    @property
    def std(self) -> float:
        return float(np.std(self._data)) if len(self._data) > 1 else 0.0

    def percentile(self, p: float) -> float:
        if not self._data:
            return 0.0
        return float(np.percentile(self._data, p))

    def zscore(self, value: float) -> float:
        std = self.std
        if std == 0:
            return 0.0
        return (value - self.mean) / std

    def snapshot(self) -> dict:
        if not self._data:
            return {"count": 0, "mean": 0.0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}
        arr = np.array(self._data)
        return {
            "count": len(arr),
            "mean": float(np.mean(arr)),
            "p50": float(np.percentile(arr, 50)),
            "p95": float(np.percentile(arr, 95)),
            "p99": float(np.percentile(arr, 99)),
            "max": float(np.max(arr)),
        }


class RateCounter:
    """Tracks events/sec over a sliding time window using timestamp buckets."""

    def __init__(self, window_seconds: float = 10.0):
        self.window_seconds = window_seconds
        self._events: Deque[float] = deque()

    def tick(self, n: int = 1, now: Optional[float] = None) -> None:
        now = now if now is not None else time.time()
        for _ in range(n):
            self._events.append(now)
        self._evict(now)

    def _evict(self, now: float) -> None:
        cutoff = now - self.window_seconds
        while self._events and self._events[0] < cutoff:
            self._events.popleft()

    def rate(self, now: Optional[float] = None) -> float:
        now = now if now is not None else time.time()
        self._evict(now)
        if not self._events:
            return 0.0
        return len(self._events) / self.window_seconds

    def seconds_since_last(self, now: Optional[float] = None) -> float:
        now = now if now is not None else time.time()
        if not self._events:
            return float("inf")
        return now - self._events[-1]
