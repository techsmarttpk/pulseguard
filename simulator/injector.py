"""Deliberate anomaly / failure injection.

This is the core of what makes PulseGuard demonstrable: every category of
bad behavior the platform claims to detect must be reproducible on demand.
Per-message injections are probabilistic (see config); episodic injections
(burst / pause / outage) are driven by a timer in `EpisodicState` and
consulted by the main loop.
"""
from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    # Import-time only (for type checkers); avoided at runtime so this
    # module never has a hard dependency on another service's identically
    # named `config` module being the one on sys.path (relevant for tests
    # that exercise multiple services' modules in one process).
    from config import SimulatorConfig


@dataclass
class InjectionOutcome:
    """Describes what the main loop should do with a generated event."""

    action: str  # "send" | "drop" | "duplicate" | "corrupt"
    delay_seconds: float = 0.0
    corrupted_payload: Optional[bytes] = None
    injected_kind: Optional[str] = None


class PerMessageInjector:
    """Applies per-message anomaly mutations to an already-generated,
    otherwise-valid event dict, in place, and decides the delivery action.
    """

    def __init__(self, cfg: SimulatorConfig):
        self.cfg = cfg

    def apply(self, event: dict) -> InjectionOutcome:
        if not self.cfg.inject_enabled:
            return InjectionOutcome(action="send")

        roll = random.random

        if roll() < self.cfg.inject_corrupted_event_prob:
            return InjectionOutcome(
                action="corrupt",
                corrupted_payload=_make_corrupted_payload(event),
                injected_kind="corrupted_event",
            )

        if roll() < self.cfg.inject_price_spike_prob:
            event["price"] = round(event["price"] * random.uniform(4, 100), 2)
            event["metadata"]["injected"] = "price_spike"

        elif roll() < self.cfg.inject_price_crash_prob:
            event["price"] = round(event["price"] * random.uniform(0.01, 0.2), 2)
            event["metadata"]["injected"] = "price_crash"

        elif roll() < self.cfg.inject_negative_price_prob:
            event["price"] = -abs(event["price"])
            event["metadata"]["injected"] = "negative_price"

        elif roll() < self.cfg.inject_zero_price_prob:
            event["price"] = 0.0
            event["metadata"]["injected"] = "zero_price"

        elif roll() < self.cfg.inject_bad_bid_ask_prob:
            # Force bid > ask, an impossible quote.
            event["bid"], event["ask"] = event["ask"] + abs(event["ask"] * 0.01) + 1, event["bid"]
            event["metadata"]["injected"] = "bad_bid_ask"

        elif roll() < self.cfg.inject_extreme_quantity_prob:
            event["quantity"] = float(random.choice([10_000_000, 50_000_000, 999_999_999]))
            event["metadata"]["injected"] = "extreme_quantity"

        if roll() < self.cfg.inject_stale_event_prob:
            event["producer_timestamp"] = time.time() - random.uniform(10, 120)
            event["metadata"]["injected"] = event["metadata"].get("injected", "") + ",stale_event"

        if roll() < self.cfg.inject_delayed_event_prob:
            delay = random.uniform(1.0, 6.0)
            event["metadata"]["injected"] = event["metadata"].get("injected", "") + ",delayed_event"
            return InjectionOutcome(action="send", delay_seconds=delay, injected_kind="delayed_event")

        if roll() < self.cfg.inject_duplicate_prob:
            event["metadata"]["injected"] = event["metadata"].get("injected", "") + ",duplicate"
            return InjectionOutcome(action="duplicate", injected_kind="duplicate")

        if roll() < self.cfg.inject_sequence_gap_prob:
            event["metadata"]["injected"] = event["metadata"].get("injected", "") + ",sequence_gap_before_this"
            return InjectionOutcome(action="send", injected_kind="sequence_gap")

        kind = event["metadata"].get("injected")
        return InjectionOutcome(action="send", injected_kind=kind)


def _make_corrupted_payload(event: dict) -> bytes:
    """Produce a deliberately malformed payload: either truncated JSON or a
    field with the wrong type, so the ingestion layer's deserialization /
    validation is exercised for real."""
    import orjson

    variant = random.choice(["truncated", "wrong_type", "missing_fields"])
    if variant == "truncated":
        raw = orjson.dumps(event)
        return raw[: max(10, len(raw) // 2)]
    if variant == "wrong_type":
        bad = dict(event)
        bad["price"] = "not-a-number"
        bad["quantity"] = "also-not-a-number"
        return orjson.dumps(bad)
    # missing_fields
    bad = dict(event)
    for f in ("bid", "ask", "sequence_number"):
        bad.pop(f, None)
    return orjson.dumps(bad)


class EpisodicState:
    """Tracks burst / pause / full-outage windows on independent timers."""

    def __init__(self, cfg: SimulatorConfig):
        self.cfg = cfg
        now = time.time()
        self._next_burst = now + cfg.inject_burst_interval_seconds
        self._next_pause = now + cfg.inject_pause_interval_seconds
        self._next_outage = now + cfg.inject_outage_interval_seconds
        self.burst_until = 0.0
        self.pause_until = 0.0
        self.outage_until = 0.0

    def tick(self, now: Optional[float] = None) -> None:
        now = now if now is not None else time.time()

        if self.cfg.inject_burst_enabled and now >= self._next_burst and now >= self.burst_until:
            self.burst_until = now + self.cfg.inject_burst_duration_seconds
            self._next_burst = self.burst_until + self.cfg.inject_burst_interval_seconds

        if self.cfg.inject_pause_enabled and now >= self._next_pause and now >= self.pause_until:
            self.pause_until = now + self.cfg.inject_pause_duration_seconds
            self._next_pause = self.pause_until + self.cfg.inject_pause_interval_seconds

        if self.cfg.inject_outage_enabled and now >= self._next_outage and now >= self.outage_until:
            self.outage_until = now + self.cfg.inject_outage_duration_seconds
            self._next_outage = self.outage_until + self.cfg.inject_outage_interval_seconds

    def throughput_multiplier(self, now: Optional[float] = None) -> float:
        now = now if now is not None else time.time()
        if now < self.outage_until:
            return 0.0
        if now < self.pause_until:
            return 0.0
        if now < self.burst_until:
            return float(self.cfg.inject_burst_multiplier)
        return 1.0

    def status(self, now: Optional[float] = None) -> str:
        now = now if now is not None else time.time()
        if now < self.outage_until:
            return "outage"
        if now < self.pause_until:
            return "pause"
        if now < self.burst_until:
            return "burst"
        return "normal"
