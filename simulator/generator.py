"""Realistic-ish per-symbol price path generation.

Each symbol is modeled as a simple geometric-Brownian-motion-style random
walk with mean-reverting volatility clustering, which produces plausible
tick-to-tick price movement (small steps, occasional volatility clusters)
without any actual market data. This is explicitly NOT a price prediction
model — it exists only to produce a believable "normal" stream that the
anomaly injector can then corrupt.
"""
from __future__ import annotations

import random
from dataclasses import dataclass

# Rough starting prices / baseline daily volatility per symbol, chosen to be
# in the right ballpark for each name without claiming any real-world
# accuracy.
BASELINE = {
    "AAPL": (195.0, 0.018),
    "MSFT": (415.0, 0.016),
    "NVDA": (118.0, 0.032),
    "GOOGL": (168.0, 0.020),
    "AMZN": (185.0, 0.022),
    "TSLA": (245.0, 0.045),
    "META": (500.0, 0.024),
}
DEFAULT_BASELINE = (100.0, 0.02)


@dataclass
class SymbolState:
    symbol: str
    price: float
    volatility: float  # instantaneous per-tick volatility, mean-reverts to baseline
    base_volatility: float
    last_bid: float
    last_ask: float

    @classmethod
    def new(cls, symbol: str) -> "SymbolState":
        start_price, base_vol = BASELINE.get(symbol, DEFAULT_BASELINE)
        tick_vol = base_vol / 200.0  # scale daily vol down to a per-tick step size
        spread = start_price * 0.0005
        return cls(
            symbol=symbol,
            price=start_price,
            volatility=tick_vol,
            base_volatility=tick_vol,
            last_bid=start_price - spread / 2,
            last_ask=start_price + spread / 2,
        )

    def step(self) -> None:
        """Advance the price by one tick using a mean-reverting-volatility
        random walk (volatility clusters, then relaxes back toward baseline)."""
        # Volatility clustering: occasionally jump vol up, otherwise decay
        # back toward the baseline.
        if random.random() < 0.01:
            self.volatility = min(self.volatility * random.uniform(1.5, 3.0), self.base_volatility * 8)
        else:
            self.volatility += (self.base_volatility - self.volatility) * 0.05

        pct_change = random.gauss(0, self.volatility)
        self.price = max(0.01, self.price * (1 + pct_change))

        spread_pct = random.uniform(0.0003, 0.0012)
        spread = self.price * spread_pct
        self.last_bid = round(self.price - spread / 2, 4)
        self.last_ask = round(self.price + spread / 2, 4)
        self.price = round(self.price, 4)

    def sample_quantity(self) -> float:
        # Log-normal-ish share sizes: mostly small round lots, occasional
        # larger blocks.
        base = random.choice([100, 200, 300, 500, 1000])
        jitter = random.uniform(0.5, 2.0)
        return round(base * jitter, 0)
