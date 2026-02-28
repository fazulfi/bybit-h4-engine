from __future__ import annotations

import asyncio
import time

from app.metrics_tm import (
    tm_last_heartbeat_age_seconds,
    tm_last_tick_age_seconds,
    tm_open_positions,
    tm_subscribed_symbols,
    tm_ws_connected,
)
from app.trade_manager.state import ManagerState


async def health_loop(state: ManagerState, log_interval_sec: int, liveness_timeout_sec: int, log) -> None:
    while True:
        now = int(time.time())
        async with state.global_lock:
            open_count = sum(len(v) for v in state.open_positions_by_symbol.values())
            subscribed_count = len(state.subscribed_symbols)
            ws_state = state.ws_state
            last_heartbeat_ts = state.last_heartbeat_ts
            last_tick_ts = state.last_tick_ts
            dropped_ticks = state.dropped_ticks

            is_stale = (
                ws_state == "CONNECTED"
                and bool(last_heartbeat_ts)
                and (now - last_heartbeat_ts) > liveness_timeout_sec
            )
            if is_stale:
                state.force_reconnect = True

        tm_ws_connected.set(1 if ws_state == "CONNECTED" else 0)
        tm_open_positions.set(open_count)
        tm_subscribed_symbols.set(subscribed_count)
        tm_last_heartbeat_age_seconds.set(max(0, now - last_heartbeat_ts) if last_heartbeat_ts else 0)
        tm_last_tick_age_seconds.set(max(0, now - last_tick_ts) if last_tick_ts else 0)

        log.info(
            "TM HEARTBEAT ws=%s open=%s subscribed=%s dropped_ticks=%s",
            ws_state,
            open_count,
            subscribed_count,
            dropped_ticks,
        )

        if is_stale:
            log.warning("TM LIVENESS stale heartbeat=%ss, scheduling reconnect", now - last_heartbeat_ts)
        if ws_state == "CONNECTED" and last_heartbeat_ts:
            if (now - last_heartbeat_ts) > liveness_timeout_sec:
                log.warning("TM LIVENESS stale heartbeat=%ss", now - last_heartbeat_ts)

        await asyncio.sleep(log_interval_sec)
