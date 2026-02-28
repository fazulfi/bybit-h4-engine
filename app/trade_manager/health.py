from __future__ import annotations

import asyncio
import time

from app.trade_manager.state import ManagerState


async def health_loop(state: ManagerState, log_interval_sec: int, liveness_timeout_sec: int, log) -> None:
    while True:
        now = int(time.time())
        async with state.global_lock:
            open_count = sum(len(v) for v in state.open_positions_by_symbol.values())
            subscribed_count = len(state.subscribed_symbols)
            ws_state = state.ws_state
            last_heartbeat_ts = state.last_heartbeat_ts
            dropped_ticks = state.dropped_ticks

            is_stale = (
                ws_state == "CONNECTED"
                and bool(last_heartbeat_ts)
                and (now - last_heartbeat_ts) > liveness_timeout_sec
            )
            if is_stale:
                state.force_reconnect = True

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
