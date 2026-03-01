from __future__ import annotations

import time

import httpx

from telegram_sidecar.config import load_settings
from telegram_sidecar.models.viewmodels import EngineViewModel


def _safe_get(data: dict, *keys, default=None):
    current = data
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
    if current is None:
        return default
    return current


async def fetch_engine_snapshot() -> EngineViewModel:
    settings = load_settings()
    url = str(settings["ENGINE_STATE_URL"])

    async with httpx.AsyncClient(timeout=2.5) as client:
        response = await client.get(url)
    payload = response.json()

    if not payload.get("ok"):
        raise RuntimeError("ENGINE_UNREACHABLE")

    state = payload.get("data", {})
    now = int(time.time())
    last_hb_ts = int(_safe_get(state, "stream", "last_ws_msg_ts", default=0) or 0)

    return EngineViewModel(
        state=str(_safe_get(state, "engine", "status", default="UNKNOWN")),
        ws="CONNECTED" if bool(_safe_get(state, "stream", "ws_connected", default=False)) else "DISCONNECTED",
        heartbeat_ms=round(float(_safe_get(state, "stream", "heartbeat_delay_ms", default=0.0) or 0.0), 1),
        dropped_5m=int(_safe_get(state, "stream", "dropped_ticks_5m", default=0) or 0),
        open_positions=int(_safe_get(state, "trading", "open_positions", default=0) or 0),
        stale=bool(last_hb_ts and (now - last_hb_ts > 30)),
    )
