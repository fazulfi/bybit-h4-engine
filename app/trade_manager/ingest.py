from __future__ import annotations

import asyncio

import aiosqlite

from app.config import load_settings
from app.db.trade_manager import (
    connect,
    fetch_new_signals,
    get_cursor,
    has_open_position_for_symbol,
    insert_virtual_position_from_signal,
    load_open_positions,
    set_cursor,
)
from app.trade_manager.state import ManagerState


async def sync_open_positions_cache(state: ManagerState) -> None:
    conn = await connect()
    try:
        open_positions = await load_open_positions(conn)
    finally:
        await conn.close()

    grouped: dict[str, list] = {}
    for p in open_positions:
        grouped.setdefault(p.symbol, []).append(p)

    async with state.global_lock:
        state.open_positions_by_symbol = grouped


async def ingest_once(state: ManagerState, batch_size: int, log) -> int:
    settings = load_settings(require_keys=False)
    conn_tm = await connect()
    conn_signals = await aiosqlite.connect(settings.signals_db)
    conn_signals.row_factory = aiosqlite.Row
    inserted_count = 0
    try:
        cursor = await get_cursor(conn_tm)
        rows = await fetch_new_signals(conn_signals, after_id=cursor, limit=batch_size)
        if not rows:
            return 0

        last_id = cursor
        for row in rows:
            last_id = int(row["id"])
            symbol = row["symbol"]
            if await has_open_position_for_symbol(conn_tm, symbol):
                log.info("TM IGNORE_OPEN_EXISTS symbol=%s signal_id=%s", symbol, row["id"])
                continue

            inserted = await insert_virtual_position_from_signal(conn_tm, row)
            if inserted:
                inserted_count += 1
                log.info("TM OPENED symbol=%s signal_id=%s", symbol, row["id"])

        await set_cursor(conn_tm, last_id)
        await conn_tm.commit()
        await sync_open_positions_cache(state)
        return inserted_count
    finally:
        await conn_signals.close()
        await conn_tm.close()


async def ingest_loop(state: ManagerState, batch_size: int, poll_sec: float, log) -> None:
    while True:
        inserted_count = await ingest_once(state, batch_size=batch_size, log=log)
        if inserted_count == 0:
            await asyncio.sleep(poll_sec)
