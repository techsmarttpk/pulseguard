"""API integration tests against a running `api` container. Requires
`make up` — skipped automatically if the API isn't reachable.
"""
import os

import httpx
import pytest

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")


def _api_reachable() -> bool:
    try:
        r = httpx.get(f"{API_BASE_URL}/health", timeout=3.0)
        return r.status_code == 200
    except Exception:
        return False


@pytest.fixture(scope="module", autouse=True)
def _skip_if_unreachable():
    if not _api_reachable():
        pytest.skip(f"PulseGuard API not reachable at {API_BASE_URL} — run `make up` first")


def test_health_endpoint():
    r = httpx.get(f"{API_BASE_URL}/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] in ("ok", "degraded")


def test_feeds_endpoint_returns_known_symbols():
    r = httpx.get(f"{API_BASE_URL}/api/feeds")
    assert r.status_code == 200
    feeds = r.json()
    assert isinstance(feeds, list)
    assert len(feeds) >= 1
    for feed in feeds:
        assert feed["state"] in ("HEALTHY", "DEGRADED", "STALE", "OFFLINE")


def test_alerts_endpoint():
    r = httpx.get(f"{API_BASE_URL}/api/alerts?limit=10")
    assert r.status_code == 200
    assert isinstance(r.json(), list)


def test_anomalies_endpoint():
    r = httpx.get(f"{API_BASE_URL}/api/anomalies?limit=10")
    assert r.status_code == 200
    assert isinstance(r.json(), list)


def test_metrics_endpoint_shape():
    r = httpx.get(f"{API_BASE_URL}/api/metrics")
    assert r.status_code == 200
    body = r.json()
    for key in (
        "messages_per_second_total",
        "p50_latency_seconds",
        "p95_latency_seconds",
        "p99_latency_seconds",
        "active_anomalies_last_5m",
        "consumer_lag_total",
    ):
        assert key in body


def test_status_endpoint():
    r = httpx.get(f"{API_BASE_URL}/api/status")
    assert r.status_code == 200
    body = r.json()
    assert body["overall_state"] in ("HEALTHY", "DEGRADED", "STALE", "OFFLINE")


def test_openapi_docs_available():
    r = httpx.get(f"{API_BASE_URL}/openapi.json")
    assert r.status_code == 200
    assert "paths" in r.json()
