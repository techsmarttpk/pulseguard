"""Rolling statistical / rule-based anomaly detection.

Complementary to the Isolation Forest: cheap, explainable, and effective at
catching the injected price spikes/crashes (large z-score / pct-change)
that the simulator produces.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from features import FeatureVector


@dataclass
class StatisticalResult:
    is_anomaly: bool
    zscore: float
    pct_change: float
    reasons: list[str]


class StatisticalDetector:
    def __init__(self, zscore_threshold: float = 4.0, pct_change_threshold: float = 0.03):
        self.zscore_threshold = zscore_threshold
        self.pct_change_threshold = pct_change_threshold

    def evaluate(self, fv: FeatureVector) -> StatisticalResult:
        reasons: list[str] = []

        zscore = 0.0
        if fv.rolling_std_price and fv.rolling_std_price > 0:
            zscore = (fv.price - fv.rolling_mean_price) / fv.rolling_std_price
            if abs(zscore) > self.zscore_threshold:
                reasons.append(f"price_zscore={zscore:.2f}")

        if abs(fv.price_return) > self.pct_change_threshold:
            reasons.append(f"pct_change={fv.price_return:.4f}")

        return StatisticalResult(
            is_anomaly=bool(reasons),
            zscore=zscore,
            pct_change=fv.price_return,
            reasons=reasons,
        )
