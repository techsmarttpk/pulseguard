from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Query, Request

import db_queries
from schemas import Anomaly

router = APIRouter(prefix="/api/anomalies", tags=["anomalies"])


@router.get("", response_model=list[Anomaly])
async def list_anomalies(
    request: Request,
    symbol: Optional[str] = None,
    method: Optional[str] = Query(None, description="statistical or isolation_forest"),
    limit: int = Query(100, le=1000),
):
    pool = request.app.state.pool
    rows = await db_queries.list_anomalies(pool, symbol=symbol, method=method, limit=limit)
    return [Anomaly(**row) for row in rows]
