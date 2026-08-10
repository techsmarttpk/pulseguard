"""Unit tests for the simulator's failure/anomaly injection (simulator/injector.py).

Each injection type must be independently reproducible (probability=1.0
for the case under test, 0 for everything else) so these tests prove every
failure mode PulseGuard claims to detect can actually be generated on
demand.
"""
import time
from dataclasses import dataclass

from injector import EpisodicState, PerMessageInjector
from generator import SymbolState


def zero_injection_config():
    """A minimal stand-in for SimulatorConfig with every injection
    probability at 0, so tests can flip on exactly one at a time."""

    @dataclass
    class Cfg:
        inject_enabled: bool = True
        inject_price_spike_prob: float = 0.0
        inject_price_crash_prob: float = 0.0
        inject_negative_price_prob: float = 0.0
        inject_zero_price_prob: float = 0.0
        inject_bad_bid_ask_prob: float = 0.0
        inject_extreme_quantity_prob: float = 0.0
        inject_duplicate_prob: float = 0.0
        inject_sequence_gap_prob: float = 0.0
        inject_stale_event_prob: float = 0.0
        inject_delayed_event_prob: float = 0.0
        inject_corrupted_event_prob: float = 0.0
        inject_burst_enabled: bool = True
        inject_burst_interval_seconds: int = 90
        inject_burst_duration_seconds: int = 5
        inject_burst_multiplier: int = 8
        inject_pause_enabled: bool = True
        inject_pause_interval_seconds: int = 120
        inject_pause_duration_seconds: int = 3
        inject_outage_enabled: bool = False
        inject_outage_interval_seconds: int = 300
        inject_outage_duration_seconds: int = 15

    return Cfg()


def make_event(price=189.42, bid=189.40, ask=189.44, quantity=100):
    return {
        "event_id": "evt_1",
        "sequence_number": 1,
        "symbol": "AAPL",
        "exchange": "NASDAQ",
        "event_type": "TRADE",
        "price": price,
        "quantity": quantity,
        "bid": bid,
        "ask": ask,
        "producer_timestamp": time.time(),
        "metadata": {},
    }


def test_price_spike_injection():
    cfg = zero_injection_config()
    cfg.inject_price_spike_prob = 1.0
    injector = PerMessageInjector(cfg)
    event = make_event(price=189.42)
    injector.apply(event)
    assert event["price"] > 189.42 * 3


def test_price_crash_injection():
    cfg = zero_injection_config()
    cfg.inject_price_crash_prob = 1.0
    injector = PerMessageInjector(cfg)
    event = make_event(price=189.42)
    injector.apply(event)
    assert event["price"] < 189.42


def test_negative_price_injection():
    cfg = zero_injection_config()
    cfg.inject_negative_price_prob = 1.0
    injector = PerMessageInjector(cfg)
    event = make_event(price=189.42)
    injector.apply(event)
    assert event["price"] < 0


def test_zero_price_injection():
    cfg = zero_injection_config()
    cfg.inject_zero_price_prob = 1.0
    injector = PerMessageInjector(cfg)
    event = make_event(price=189.42)
    injector.apply(event)
    assert event["price"] == 0.0


def test_bad_bid_ask_injection():
    cfg = zero_injection_config()
    cfg.inject_bad_bid_ask_prob = 1.0
    injector = PerMessageInjector(cfg)
    event = make_event(bid=189.40, ask=189.44)
    injector.apply(event)
    assert event["bid"] > event["ask"]


def test_extreme_quantity_injection():
    cfg = zero_injection_config()
    cfg.inject_extreme_quantity_prob = 1.0
    injector = PerMessageInjector(cfg)
    event = make_event(quantity=100)
    injector.apply(event)
    assert event["quantity"] >= 10_000_000


def test_duplicate_injection_signals_action():
    cfg = zero_injection_config()
    cfg.inject_duplicate_prob = 1.0
    injector = PerMessageInjector(cfg)
    outcome = injector.apply(make_event())
    assert outcome.action == "duplicate"


def test_stale_event_injection_pushes_timestamp_into_past():
    cfg = zero_injection_config()
    cfg.inject_stale_event_prob = 1.0
    injector = PerMessageInjector(cfg)
    event = make_event()
    now = time.time()
    injector.apply(event)
    assert (now - event["producer_timestamp"]) > 5.0


def test_delayed_event_injection_returns_delay():
    cfg = zero_injection_config()
    cfg.inject_delayed_event_prob = 1.0
    injector = PerMessageInjector(cfg)
    outcome = injector.apply(make_event())
    assert outcome.delay_seconds > 0


def test_corrupted_event_injection_returns_raw_bytes():
    cfg = zero_injection_config()
    cfg.inject_corrupted_event_prob = 1.0
    injector = PerMessageInjector(cfg)
    outcome = injector.apply(make_event())
    assert outcome.action == "corrupt"
    assert isinstance(outcome.corrupted_payload, (bytes, bytearray))


def test_no_injection_when_disabled():
    cfg = zero_injection_config()
    cfg.inject_price_spike_prob = 1.0
    cfg.inject_enabled = False
    injector = PerMessageInjector(cfg)
    event = make_event(price=189.42)
    injector.apply(event)
    assert event["price"] == 189.42


def test_episodic_burst_multiplies_throughput():
    cfg = zero_injection_config()
    cfg.inject_burst_enabled = True
    cfg.inject_burst_interval_seconds = 0
    cfg.inject_burst_duration_seconds = 10
    cfg.inject_burst_multiplier = 8
    cfg.inject_pause_enabled = False
    cfg.inject_outage_enabled = False
    state = EpisodicState(cfg)
    now = time.time() + 0.01
    state.tick(now)
    assert state.throughput_multiplier(now) == 8.0
    assert state.status(now) == "burst"


def test_episodic_pause_stops_throughput():
    cfg = zero_injection_config()
    cfg.inject_pause_enabled = True
    cfg.inject_pause_interval_seconds = 0
    cfg.inject_pause_duration_seconds = 10
    cfg.inject_burst_enabled = False
    cfg.inject_outage_enabled = False
    state = EpisodicState(cfg)
    now = time.time() + 0.01
    state.tick(now)
    assert state.throughput_multiplier(now) == 0.0
    assert state.status(now) == "pause"


def test_generator_produces_positive_prices():
    state = SymbolState.new("AAPL")
    for _ in range(500):
        state.step()
        assert state.price > 0
        assert state.last_bid <= state.last_ask
