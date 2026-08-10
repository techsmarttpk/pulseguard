"""Per-symbol feature engineering shared by both the statistical detector
and the Isolation Forest detector, so every event's features are computed
exactly once.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "common"))
from pulseguard_common.stats import RollingWindow  # noqa: E402


@dataclass
class FeatureVector:
    symbol: str
    price: float
    price_return: float
    quantity: float
    spread: float
    spread_pct: float
    inter_arrival_seconds: float
    rolling_volatility: float
    price_deviation_from_mean: float
    latency_seconds: float
    rolling_mean_price: float
    rolling_std_price: float

    def as_array(self) -> list[float]:
        return [
            self.price_return,
            self.quantity,
            self.spread,
            self.spread_pct,
            self.inter_arrival_seconds,
            self.rolling_volatility,
            self.price_deviation_from_mean,
            self.latency_seconds,
        ]


FEATURE_NAMES = [
    "price_return",
    "quantity",
    "spread",
    "spread_pct",
    "inter_arrival_seconds",
    "rolling_volatility",
    "price_deviation_from_mean",
    "latency_seconds",
]


class _SymbolState:
    def __init__(self, window_size: int):
        self.last_price: Optional[float] = None
        self.last_event_time: Optional[float] = None
        self.price_window = RollingWindow(maxlen=window_size)
        self.return_window = RollingWindow(maxlen=window_size)


class FeatureTracker:
    def __init__(self, window_size: int = 100):
        self.window_size = window_size
        self._states: dict[str, _SymbolState] = {}

    def _state(self, symbol: str) -> _SymbolState:
        return self._states.setdefault(symbol, _SymbolState(self.window_size))

    def observe(self, event: dict, now: Optional[float] = None) -> FeatureVector:
        now = now if now is not None else time.time()
        symbol = event["symbol"]
        state = self._state(symbol)

        price = float(event["price"])
        quantity = float(event.get("quantity") or 0.0)
        bid = float(event.get("bid") or 0.0)
        ask = float(event.get("ask") or 0.0)
        producer_ts = float(event["producer_timestamp"])

        price_return = 0.0
        if state.last_price and state.last_price != 0:
            price_return = (price - state.last_price) / state.last_price

        inter_arrival = 0.0
        if state.last_event_time is not None:
            inter_arrival = max(0.0, now - state.last_event_time)

        spread = ask - bid
        spread_pct = spread / price if price else 0.0
        latency = max(0.0, now - producer_ts)

        rolling_mean_price = state.price_window.mean or price
        rolling_std_price = state.price_window.std
        price_deviation = (price - rolling_mean_price) / rolling_mean_price if rolling_mean_price else 0.0
        rolling_volatility = state.return_window.std

        fv = FeatureVector(
            symbol=symbol,
            price=price,
            price_return=price_return,
            quantity=quantity,
            spread=spread,
            spread_pct=spread_pct,
            inter_arrival_seconds=inter_arrival,
            rolling_volatility=rolling_volatility,
            price_deviation_from_mean=price_deviation,
            latency_seconds=latency,
            rolling_mean_price=rolling_mean_price,
            rolling_std_price=rolling_std_price,
        )

        # Update state AFTER computing features so the current event is
        # compared against history, not against itself.
        state.price_window.add(price)
        state.return_window.add(price_return)
        state.last_price = price
        state.last_event_time = now

        return fv
