"""Unit tests for producer `acks` configuration validation
(services/common/pulseguard_common/kafka_utils.py).

Regression coverage for a real production incident: `make_producer`
defaulted to `acks="1"` (a *string*), which aiokafka's own validation
rejects (`if acks not in (0, 1, -1, 'all', _missing)` — the string "1" is
never equal to the int 1), crashing every producer-owning service
(simulator, ingestion, anomaly_detection, monitoring) at startup with
`ValueError: Invalid ACKS parameter`. There was a second, independent bug
underneath it: aiokafka also requires `acks` to be `-1`/`'all'` whenever
`enable_idempotence=True`, so even fixing only the type would have hit a
*different* ValueError. `validate_acks_config` is exercised directly here
(no live Kafka broker needed) so both classes of misconfiguration are
caught by CI, not just discovered at container startup.
"""
import pytest

from pulseguard_common.kafka_utils import DEFAULT_ACKS, VALID_ACKS, validate_acks_config


def test_default_acks_is_a_valid_int():
    # Regression guard: the default must be a real int in VALID_ACKS, not
    # a string like the original "1" that shipped.
    assert isinstance(DEFAULT_ACKS, int)
    assert DEFAULT_ACKS in VALID_ACKS


def test_current_production_default_is_valid():
    # This is the exact (acks, enable_idempotence) pair make_producer()
    # uses by default for every PulseGuard service — must never raise.
    validate_acks_config(DEFAULT_ACKS, enable_idempotence=True)


@pytest.mark.parametrize("acks", [0, 1, -1])
def test_valid_int_acks_without_idempotence(acks):
    validate_acks_config(acks, enable_idempotence=False)


def test_string_acks_is_rejected():
    # The exact original bug: acks="1" (str) must not silently pass.
    with pytest.raises(ValueError):
        validate_acks_config("1", enable_idempotence=False)


def test_string_all_is_rejected_in_our_int_only_scheme():
    # aiokafka itself accepts the string 'all' as an alias for -1, but we
    # deliberately standardize on ints everywhere in PulseGuard to remove
    # this whole class of str/int mismatch bug — 'all' should be rejected
    # by our validator even though aiokafka would accept it.
    with pytest.raises(ValueError):
        validate_acks_config("all", enable_idempotence=False)


def test_out_of_range_acks_is_rejected():
    with pytest.raises(ValueError):
        validate_acks_config(2, enable_idempotence=False)


def test_acks_one_with_idempotence_is_rejected():
    # acks=1 is individually valid, but not in combination with
    # enable_idempotence=True — this is the second bug that would have
    # surfaced immediately after naively fixing only the string/int issue.
    with pytest.raises(ValueError):
        validate_acks_config(1, enable_idempotence=True)


def test_acks_zero_with_idempotence_is_rejected():
    with pytest.raises(ValueError):
        validate_acks_config(0, enable_idempotence=True)


def test_acks_minus_one_with_idempotence_is_valid():
    validate_acks_config(-1, enable_idempotence=True)
