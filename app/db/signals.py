from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import aiosqlite

from app.config import load_settings
from app.timeutil import now_utc_s


# Row:
# (symbol, timeframe, date, signal_type, side, entry, stop, tp, created_at)
SignalRow = Tuple[
    str, str, int, str, str, float, float, float, int
]

async def _connect() -> aiosqlite.Connection:
    s = load_settings(require_keys=False)
    conn = await aiosqlite.connect(s.signals_db)
    await conn.execute("PRAGMA journal_mode=WAL;")
    await conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


async def insert_signal_if_new(
    symbol: str,
    timeframe: str,
    date: int,  # candle CLOSE time (seconds UTC)
    signal_type: str,
    side: str,
    entry: float,
    stop: float,
    tp: float,
    extra: Optional[Dict[str, float]] = None,
) -> bool:
    """
    Insert signal if not exists (idempotent via PK).
    Return True if inserted, False if already existed.
    """
    extra = extra or {}

    sql = """
    INSERT INTO signals(
        symbol, timeframe, date,
        signal_type, side,
        entry, stop, tp,
        rvol, atr14, atr_pct, hh20, ll20, volume, close,
        created_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(symbol, timeframe, date, signal_type) DO NOTHING
    """

    conn = await _connect()
    try:
        cur = await conn.execute(
            sql,
            (
                symbol,
                timeframe,
                date,
                signal_type,
                side,
                float(entry),
                float(stop),
                float(tp),
                float(extra.get("rvol", 0.0)),
                float(extra.get("atr14", 0.0)),
                float(extra.get("atr_pct", 0.0)),
                float(extra.get("hh20", 0.0)),
                float(extra.get("ll20", 0.0)),
                float(extra.get("volume", 0.0)),
                float(extra.get("close", 0.0)),
                now_utc_s(),
            ),
        )
        await conn.commit()

        # rowcount = 1 berarti insert sukses
        return cur.rowcount == 1
    finally:
        await conn.close()

async def get_recent_signals(limit: int = 200) -> List[SignalRow]:
    """
    Get recent signals ordered by created_at ASC.
    """
    conn = await _connect()
    try:
        cur = await conn.execute(
            """
            SELECT
                symbol, timeframe, date,
                signal_type, side,
                entry, stop, tp,
                created_at
            FROM signals
            ORDER BY created_at ASC
            LIMIT ?
            """,
            (limit,),
        )
        rows = await cur.fetchall()
        return [
            (
                r[0],
                r[1],
                int(r[2]),
                r[3],
                r[4],
                float(r[5]),
                float(r[6]),
                float(r[7]),
                int(r[8]),
            )
            for r in rows
        ]
    finally:
        await conn.close()



async def get_signal(
    symbol: str,
    timeframe: str,
    date: int,
    signal_type: str,
) -> Optional[SignalRow]:
    conn = await _connect()
    try:
        cur = await conn.execute(
            """
            SELECT
                symbol, timeframe, date,
                signal_type, side,
                entry, stop, tp,
                status, created_at
            FROM signals
            WHERE symbol=? AND timeframe=? AND date=? AND signal_type=?
            LIMIT 1
            """,
            (symbol, timeframe, date, signal_type),
        )
        r = await cur.fetchone()
        if not r:
            return None
        return (
            r[0],
            r[1],
            int(r[2]),
            r[3],
            r[4],
            float(r[5]),
            float(r[6]),
            float(r[7]),
            r[8],
            int(r[9]),
        )
    finally:
        await conn.close()
