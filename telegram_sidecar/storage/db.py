from __future__ import annotations

import aiosqlite

from telegram_sidecar.config import load_settings


async def get_open_signals(limit: int = 5, offset: int = 0) -> list[dict]:
    settings = load_settings()
    db_path = str(settings["DB_PATH"])

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT sid, symbol, strategy, direction, opened_ts, pnl_pct
            FROM signals
            WHERE status = 'OPEN'
            ORDER BY opened_ts DESC
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
        )
        rows = await cursor.fetchall()
    return [dict(r) for r in rows]
