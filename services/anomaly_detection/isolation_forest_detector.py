"""Isolation Forest anomaly detector, used as an experimental complement to
the deterministic validation and statistical detectors above — NOT as a
sole/authoritative production surveillance signal. It is retrained
periodically from a rolling buffer of recent "mostly normal" feature
vectors, since market regimes drift over the life of a long-running feed.

Trade-off, documented here and in the README: training on the live stream
(rather than a curated clean dataset) means injected anomalies that happen
to land in the training buffer can slightly dull the model's sensitivity.
We mitigate this by training on a large buffer relative to the injection
rate, and by always keeping the statistical + validation detectors as the
primary line of defense.
"""
from __future__ import annotations

from collections import deque
from typing import Optional

import numpy as np
from sklearn.ensemble import IsolationForest

from features import FEATURE_NAMES, FeatureVector


class IsolationForestDetector:
    def __init__(
        self,
        contamination: float = 0.02,
        retrain_every_n: int = 2000,
        min_train_samples: int = 300,
        buffer_size: int = 5000,
    ):
        self.contamination = contamination
        self.retrain_every_n = retrain_every_n
        self.min_train_samples = min_train_samples
        self.buffer: deque[list[float]] = deque(maxlen=buffer_size)
        self.model: Optional[IsolationForest] = None
        self._since_retrain = 0

    def _maybe_retrain(self) -> None:
        self._since_retrain += 1
        ready = len(self.buffer) >= self.min_train_samples
        due = self.model is None or self._since_retrain >= self.retrain_every_n
        if ready and due:
            X = np.array(self.buffer, dtype=float)
            model = IsolationForest(
                n_estimators=100,
                contamination=self.contamination,
                random_state=42,
                n_jobs=1,
            )
            model.fit(X)
            self.model = model
            self._since_retrain = 0

    def observe_and_score(self, fv: FeatureVector) -> tuple[bool, float]:
        """Feed a feature vector into the training buffer and, if a model
        is available, score it. Returns (is_anomaly, anomaly_score) where a
        more negative score means more anomalous (sklearn convention)."""
        vec = fv.as_array()
        self.buffer.append(vec)
        self._maybe_retrain()

        if self.model is None:
            return False, 0.0

        X = np.array([vec], dtype=float)
        prediction = self.model.predict(X)[0]  # -1 anomaly, 1 normal
        score = float(self.model.decision_function(X)[0])
        # sklearn returns numpy scalar types (np.int64/np.bool_) here, which
        # are not instances of Python's own `bool`/are not directly
        # JSON-serializable by orjson in a Postgres/Prometheus-label-safe
        # way — cast explicitly so callers get plain Python types.
        return bool(prediction == -1), score

    @property
    def feature_names(self) -> list[str]:
        return FEATURE_NAMES
