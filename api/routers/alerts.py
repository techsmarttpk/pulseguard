from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Query, Request

import db_queries
from schemas import Alert

router = APIRouter(prefix="/api/alerts", tags=["alerts"])


@router.get("", response_model=list[Alert])
async def list_alerts(
    request: Request,
    status: Optional[str] = Query(None, description="ACTIVE or RESOLVED"),
    severity: Optional[str] = Query(None, description="CRITICAL, HIGH, MEDIUM, or LOW"),
    feed: Optional[str] = None,
    limit: int = Query(100, le=1000),
):
    pool = request.app.state.pool
    rows = await db_queries.list_alerts(pool, status=status, severity=severity, feed=feed, limit=limit)
    return [Alert(**row) for row in rows]
