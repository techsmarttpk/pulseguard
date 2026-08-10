"""Minimal Prometheus HTTP API client for the handful of instant queries the
dashboard needs live (messages/sec, latency percentiles, consumer lag).
Historical/aggregated data (alerts, anomalies, feed transitions) comes from
Postgres instead — Prometheus here is used exactly the way it's meant to
be: for recent operational metrics, not as a system of record.
"""
from __future__ import annotations

import httpx


class PrometheusClient:
    def __init__(self, base_url: str, timeout: float = 5.0):
        self.base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(timeout=timeout)

    async def instant_query(self, query: str) -> list[dict]:
        try:
            resp = await self._client.get(f"{self.base_url}/api/v1/query", params={"query": query})
            resp.raise_for_status()
            data = resp.json()
            if data.get("status") != "success":
                return []
            return data["data"]["result"]
        except Exception:
            return []

    async def scalar(self, query: str, default: float = 0.0) -> float:
        result = await self.instant_query(query)
        if not result:
            return default
        try:
            return float(result[0]["value"][1])
        except (KeyError, IndexError, ValueError, TypeError):
            return default

    async def by_label(self, query: str, label: str) -> dict[str, float]:
        result = await self.instant_query(query)
        out: dict[str, float] = {}
        for item in result:
            key = item.get("metric", {}).get(label, "unknown")
            try:
                out[key] = float(item["value"][1])
            except (KeyError, IndexError, ValueError, TypeError):
                continue
        return out

    async def close(self):
        await self._client.aclose()
