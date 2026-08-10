"""PulseGuard REST API (FastAPI).

Reads, never writes: this service is a thin, async read layer over
Postgres (alerts, anomalies, feed transitions — the system-of-record for
history) and Prometheus (live rates/latency/lag — the system-of-record for
"right now"). All detection/write logic lives in the ingestion,
anomaly_detection and monitoring services.
"""
from __future__ import annotations

import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "services" / "common"))

from fastapi import FastAPI, Request  # noqa: E402
from fastapi.middleware.cors import CORSMiddleware  # noqa: E402
from fastapi.responses import ORJSONResponse, Response  # noqa: E402
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest  # noqa: E402

from pulseguard_common.db import make_pool  # noqa: E402
from pulseguard_common.logging_utils import configure_logging  # noqa: E402

from config import load_config  # noqa: E402
from prom_client import PrometheusClient  # noqa: E402
from routers import alerts, anomalies, feeds, metrics, status  # noqa: E402

START_TIME = time.time()

# The API is a read-only layer with no `pulseguard_messages_*` /
# `pulseguard_anomalies_*` style business metrics of its own — those are
# owned by ingestion/anomaly_detection/monitoring, which are already
# scraped. What the API *does* meaningfully own is its own HTTP layer:
# request volume, status codes, and latency per route. That's real,
# non-fabricated data (this is what Prometheus was 404'ing while trying to
# scrape), and it's useful observability in its own right — if the
# dashboard API itself starts erroring or slowing down, that should be
# visible the same way every other service's health is.
API_HTTP_REQUESTS_TOTAL = Counter(
    "pulseguard_api_http_requests_total",
    "Total HTTP requests handled by the PulseGuard API",
    ["method", "path", "status_code"],
)
API_HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "pulseguard_api_http_request_duration_seconds",
    "PulseGuard API request duration in seconds",
    ["method", "path"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    cfg = load_config()
    log = configure_logging("api")
    app.state.cfg = cfg
    app.state.log = log
    app.state.pool = await make_pool(cfg.database_url)
    app.state.prom = PrometheusClient(cfg.prometheus_url)
    log.info("api_started", port=cfg.port)
    yield
    await app.state.pool.close()
    await app.state.prom.close()
    log.info("api_stopped")


app = FastAPI(
    title="PulseGuard API",
    description=(
        "Operational API for PulseGuard, a market-data reliability and "
        "anomaly-detection platform. Serves feed health, alerts, "
        "anomalies, and aggregated metrics to the dashboard."
    ),
    version="1.0.0",
    default_response_class=ORJSONResponse,
    lifespan=lifespan,
)

_cfg_for_cors = load_config()
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cfg_for_cors.cors_origin_list,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(feeds.router)
app.include_router(alerts.router)
app.include_router(anomalies.router)
app.include_router(metrics.router)
app.include_router(status.router)


@app.middleware("http")
async def prometheus_http_metrics(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    duration = time.perf_counter() - start

    # Use the matched route's path *template* (e.g. "/api/feeds/{feed_id}"),
    # not the raw URL, so metric cardinality stays bounded regardless of
    # how many distinct symbols/ids get requested. Falls back to the raw
    # path for genuinely unmatched routes (404s).
    route = request.scope.get("route")
    path_label = route.path if route is not None else request.url.path

    API_HTTP_REQUESTS_TOTAL.labels(
        method=request.method, path=path_label, status_code=response.status_code
    ).inc()
    API_HTTP_REQUEST_DURATION_SECONDS.labels(method=request.method, path=path_label).observe(duration)
    return response


@app.get("/metrics", tags=["health"], include_in_schema=False)
async def metrics_endpoint():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health", tags=["health"])
async def health():
    db_ok = True
    try:
        async with app.state.pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
    except Exception:
        db_ok = False
    return {
        "status": "ok" if db_ok else "degraded",
        "database": "ok" if db_ok else "unreachable",
        "uptime_seconds": round(time.time() - START_TIME, 1),
    }
