from __future__ import annotations

from fastapi import APIRouter

from telegram_sidecar.api.services import fetch_engine_snapshot
from telegram_sidecar.storage.db import get_open_signals

router = APIRouter()


@router.get("/health")
async def health() -> dict:
    return {"ok": True}


@router.get("/snapshot/engine")
async def snapshot_engine() -> dict:
    try:
        vm = await fetch_engine_snapshot()
        return {"ok": True, "data": vm.__dict__}
    except Exception as exc:  # fallback envelope for degraded mode
        return {"ok": False, "code": "ENGINE_UNREACHABLE", "message": str(exc)}


@router.get("/signals/open")
async def signals_open(p: int = 1, ps: int = 5) -> dict:
    page = max(1, p)
    page_size = min(20, max(1, ps))
    rows = await get_open_signals(limit=page_size, offset=(page - 1) * page_size)
    return {"ok": True, "data": {"page": page, "page_size": page_size, "items": rows}}
