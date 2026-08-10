"""Deterministic validation engine, run on every market-data message before
any statistical/ML anomaly detection. Pure functions + small per-symbol
tracking state so this module is trivially unit-testable without Kafka.
"""
from __future__ import annotations

import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ValidationResult:
    is_valid: bool
    reasons: list[str] = field(default_factory=list)
    is_duplicate: bool = False
    is_sequence_gap: bool = False
    gap_size: int = 0
    is_stale: bool = False


class DuplicateCache:
    """Bounded FIFO membership cache (approximate LRU) used to detect
    duplicate event_ids without retaining unbounded memory."""

    def __init__(self, max_size: int = 50_000):
        self.max_size = max_size
        self._seen: "OrderedDict[str, None]" = OrderedDict()

    def contains_and_add(self, event_id: str) -> bool:
        if event_id in self._seen:
            self._seen.move_to_end(event_id)
            return True
        self._seen[event_id] = None
        if len(self._seen) > self.max_size:
            self._seen.popitem(last=False)
        return False


class SymbolSequenceTracker:
    """Tracks last-seen sequence number per symbol to detect gaps."""

    def __init__(self):
        self._last_seq: dict[str, int] = {}

    def observe(self, symbol: str, seq: int) -> tuple[bool, int]:
        """Returns (is_gap, gap_size). Updates state regardless, so a gap is
        only reported once even if messages keep arriving after it."""
        last = self._last_seq.get(symbol)
        self._last_seq[symbol] = max(seq, last) if last is not None else seq
        if last is None:
            return False, 0
        expected = last + 1
        if seq == expected:
            return False, 0
        if seq > expected:
            return True, seq - expected
        return False, 0  # out-of-order / late arrival, not treated as a gap


class ValidationEngine:
    def __init__(
        self,
        stale_threshold_seconds: float = 5.0,
        extreme_quantity_threshold: float = 1_000_000,
        duplicate_cache_size: int = 50_000,
    ):
        self.stale_threshold_seconds = stale_threshold_seconds
        self.extreme_quantity_threshold = extreme_quantity_threshold
        self.dup_cache = DuplicateCache(duplicate_cache_size)
        self.seq_tracker = SymbolSequenceTracker()

    def validate(self, event: dict, now: Optional[float] = None) -> ValidationResult:
        now = now if now is not None else time.time()
        reasons: list[str] = []

        price = event.get("price")
        bid = event.get("bid")
        ask = event.get("ask")
        quantity = event.get("quantity")

        if price is None or not isinstance(price, (int, float)) or price <= 0:
            reasons.append("non_positive_or_missing_price")

        if bid is not None and ask is not None and isinstance(bid, (int, float)) and isinstance(ask, (int, float)):
            if bid > ask:
                reasons.append("bid_greater_than_ask")

        if quantity is not None and isinstance(quantity, (int, float)):
            if quantity <= 0:
                reasons.append("non_positive_quantity")
            elif quantity > self.extreme_quantity_threshold:
                reasons.append("extreme_quantity")

        is_duplicate = self.dup_cache.contains_and_add(event.get("event_id", ""))
        if is_duplicate:
            reasons.append("duplicate_event_id")

        is_gap, gap_size = self.seq_tracker.observe(event.get("symbol", ""), event.get("sequence_number", 0))

        producer_ts = event.get("producer_timestamp")
        is_stale = False
        if isinstance(producer_ts, (int, float)):
            if (now - producer_ts) > self.stale_threshold_seconds:
                is_stale = True
                reasons.append("stale_event")

        # Duplicate and stale are flagged/counted separately; they don't by
        # themselves make the underlying data structurally invalid.
        hard_failure_reasons = {
            "non_positive_or_missing_price",
            "bid_greater_than_ask",
            "non_positive_quantity",
            "extreme_quantity",
        }
        is_valid = not any(r in hard_failure_reasons for r in reasons)

        return ValidationResult(
            is_valid=is_valid,
            reasons=reasons,
            is_duplicate=is_duplicate,
            is_sequence_gap=is_gap,
            gap_size=gap_size,
            is_stale=is_stale,
        )
