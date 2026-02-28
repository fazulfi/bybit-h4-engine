from __future__ import annotations

import asyncio

from app.trade_manager.config import load_trade_manager_config
from app.trade_manager.health import health_loop
from app.trade_manager.ingest import ingest_loop, ingest_once, sync_open_positions_cache
from app.trade_manager.state import ManagerState
from app.trade_manager.ws_client import tick_worker_loop, ws_loop


async def run_trade_manager(once: bool, log) -> None:
    cfg = load_trade_manager_config()
    state = ManagerState()

    await sync_open_positions_cache(state)

    if once:
        await ingest_once(state, batch_size=cfg.ingest_batch_size, log=log)
        return

    await asyncio.gather(
        ingest_loop(state, batch_size=cfg.ingest_batch_size, poll_sec=cfg.ingest_poll_sec, log=log),
        ws_loop(state, ws_url=cfg.ws_url, hit_mode=cfg.trigger_price_mode, log=log),
        tick_worker_loop(state, hit_mode=cfg.trigger_price_mode, log=log),
        health_loop(
            state,
            log_interval_sec=cfg.health_log_sec,
            liveness_timeout_sec=cfg.liveness_timeout_sec,
            log=log,
        ),
    )
