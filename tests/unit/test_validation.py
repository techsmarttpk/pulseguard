"""Unit tests for the deterministic validation engine (services/ingestion)."""
import time

from validation import ValidationEngine


def make_event(**overrides):
    base = {
        "event_id": "evt_1",
        "sequence_number": 1,
        "symbol": "AAPL",
        "exchange": "NASDAQ",
        "event_type": "TRADE",
        "price": 189.42,
        "quantity": 100,
        "bid": 189.40,
        "ask": 189.44,
        "producer_timestamp": time.time(),
    }
    base.update(overrides)
    return base


def test_valid_event_passes():
    engine = ValidationEngine()
    result = engine.validate(make_event())
    assert result.is_valid
    assert result.reasons == []


def test_negative_price_is_invalid():
    engine = ValidationEngine()
    result = engine.validate(make_event(price=-5.0))
    assert not result.is_valid
    assert "non_positive_or_missing_price" in result.reasons


def test_zero_price_is_invalid():
    engine = ValidationEngine()
    result = engine.validate(make_event(price=0.0))
    assert not result.is_valid


def test_bid_greater_than_ask_is_invalid():
    engine = ValidationEngine()
    result = engine.validate(make_event(bid=200.0, ask=190.0))
    assert not result.is_valid
    assert "bid_greater_than_ask" in result.reasons


def test_extreme_quantity_is_invalid():
    engine = ValidationEngine(extreme_quantity_threshold=1000)
    result = engine.validate(make_event(quantity=50_000))
    assert not result.is_valid
    assert "extreme_quantity" in result.reasons


def test_duplicate_event_id_detected_but_not_hard_invalid():
    engine = ValidationEngine()
    first = engine.validate(make_event(event_id="evt_dup", sequence_number=1))
    second = engine.validate(make_event(event_id="evt_dup", sequence_number=2))
    assert first.is_valid and not first.is_duplicate
    assert second.is_duplicate
    # A duplicate resend of otherwise-good data isn't "structurally invalid".
    assert second.is_valid


def test_sequence_gap_detected():
    engine = ValidationEngine()
    engine.validate(make_event(event_id="e1", sequence_number=1))
    result = engine.validate(make_event(event_id="e2", sequence_number=5))
    assert result.is_sequence_gap
    assert result.gap_size == 3  # expected 2, got 5 -> gap of 3


def test_no_gap_on_consecutive_sequence():
    engine = ValidationEngine()
    engine.validate(make_event(event_id="e1", sequence_number=1))
    result = engine.validate(make_event(event_id="e2", sequence_number=2))
    assert not result.is_sequence_gap


def test_stale_event_detected():
    engine = ValidationEngine(stale_threshold_seconds=5.0)
    old_event = make_event(producer_timestamp=time.time() - 60)
    result = engine.validate(old_event)
    assert result.is_stale
    assert "stale_event" in result.reasons


def test_fresh_event_not_stale():
    engine = ValidationEngine(stale_threshold_seconds=5.0)
    result = engine.validate(make_event(producer_timestamp=time.time()))
    assert not result.is_stale
