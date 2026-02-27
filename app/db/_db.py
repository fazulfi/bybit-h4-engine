from __future__ import annotations

import aiosqlite
from typing import Any, Iterable

from app.config import load_settings


from pathlib import Path
import os

def _db_path(filename: str) -> str:
    s = load_settings(require_keys=False)
    base = getattr(s, "db_dir", Path("database"))
    return os.fspath(Path(base) / filename)

async def fetch_all(db_file: str, sql: str, params: Iterable[Any] = ()) -> list[tuple]:
    path = _db_path(db_file)
    async with aiosqlite.connect(path) as db:
        cur = await db.execute(sql, tuple(params))
        rows = await cur.fetchall()
        await cur.close()
        return rows


async def fetch_one(db_file: str, sql: str, params: Iterable[Any] = ()) -> tuple | None:
    path = _db_path(db_file)
    async with aiosqlite.connect(path) as db:
        cur = await db.execute(sql, tuple(params))
        row = await cur.fetchone()
        await cur.close()
        return row
