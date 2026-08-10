from __future__ import annotations

from fastapi import APIRouter, Request

import db_queries
from schemas import SystemStatus

router = APIRouter(prefix="/api/status", tags=["status"])

_STATE_RANK = {"OFFLINE": 0, "STALE": 1, "DEGRADED": 2, "HEALTHY": 3}


@router.get("", response_model=SystemStatus)
async def get_status(request: Request):
    pool = request.app.state.pool
    symbols = request.app.state.cfg.symbol_list

    states = await db_queries.latest_feed_states(pool)
    counts = {"HEALTHY": 0, "DEGRADED": 0, "STALE": 0, "OFFLINE": 0}
    worst = "HEALTHY"
    for symbol in symbols:
        state = states.get(symbol, {}).get("state", "OFFLINE")
        counts[state] = counts.get(state, 0) + 1
        if _STATE_RANK.get(state, 0) < _STATE_RANK.get(worst, 3):
            worst = state

    active = await db_queries.active_alert_counts(pool)
    for sev in ("CRITICAL", "HIGH", "MEDIUM", "LOW"):
        active.setdefault(sev, 0)

    return SystemStatus(
        overall_state=worst,
        feeds_healthy=counts["HEALTHY"],
        feeds_degraded=counts["DEGRADED"],
        feeds_stale=counts["STALE"],
        feeds_offline=counts["OFFLINE"],
        active_alerts_by_severity=active,
    )
