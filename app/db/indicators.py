from __future__ import annotations

from typing import Dict, Optional, Tuple

import aiosqlite

from app.config import load_settings

from app.db._db import fetch_one

from app.db.prices import get_all_dates

# Row: (date, atr14, atr_pct, hh20, ll20, avg_vol20, rvol)
IndicatorRow = Tuple[int, float, float, float, float, float, float]


async def _connect() -> aiosqlite.Connection:
    s = load_settings(require_keys=False)
    conn = await aiosqlite.connect(s.indicators_db)
    await conn.execute("PRAGMA journal_mode=WAL;")
    await conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


async def upsert_indicator(
    symbol: str,
    timeframe: str,
    date: int,  # candle OPEN time (seconds UTC), same as prices.date
    values: Dict[str, float],
) -> None:
    """
    Insert/update indicator row for a candle.
    Expected keys in values:
      atr14, atr_pct, hh20, ll20, avg_vol20, rvol
    """
    sql = """
    INSERT INTO indicators(symbol, timeframe, date, atr14, atr_pct, hh20, ll20, avg_vol20, rvol)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(symbol, timeframe, date) DO UPDATE SET
      atr14=excluded.atr14,
      atr_pct=excluded.atr_pct,
      hh20=excluded.hh20,
      ll20=excluded.ll20,
      avg_vol20=excluded.avg_vol20,
      rvol=excluded.rvol
    """
    conn = await _connect()
    try:
        await conn.execute(
            sql,
            (
                symbol,
                timeframe,
                date,
                float(values.get("atr14", 0.0)),
                float(values.get("atr_pct", 0.0)),
                float(values.get("hh20", 0.0)),
                float(values.get("ll20", 0.0)),
                float(values.get("avg_vol20", 0.0)),
                float(values.get("rvol", 0.0)),
            ),
        )
        await conn.commit()
    finally:
        await conn.close()


async def has_indicator(symbol: str, timeframe: str, date: int) -> bool:
    conn = await _connect()
    try:
        cur = await conn.execute(
            "SELECT 1 FROM indicators WHERE symbol=? AND timeframe=? AND date=? LIMIT 1",
            (symbol, timeframe, date),
        )
        row = await cur.fetchone()
        return row is not None
    finally:
        await conn.close()


async def get_latest_indicator(symbol: str, timeframe: str) -> Optional[IndicatorRow]:
    conn = await _connect()
    try:
        cur = await conn.execute(
            """
            SELECT date, atr14, atr_pct, hh20, ll20, avg_vol20, rvol
            FROM indicators
            WHERE symbol=? AND timeframe=?
            ORDER BY date DESC
            LIMIT 1
            """,
            (symbol, timeframe),
        )
        r = await cur.fetchone()
        if not r:
            return None
        return (
            int(r[0]),
            float(r[1] or 0.0),
            float(r[2] or 0.0),
            float(r[3] or 0.0),
            float(r[4] or 0.0),
            float(r[5] or 0.0),
            float(r[6] or 0.0),
        )
    finally:
        await conn.close()


async def get_indicator(symbol: str, timeframe: str, date: int) -> Optional[IndicatorRow]:
    conn = await _connect()
    try:
        cur = await conn.execute(
            """
            SELECT date, atr14, atr_pct, hh20, ll20, avg_vol20, rvol
            FROM indicators
            WHERE symbol=? AND timeframe=? AND date=?
            LIMIT 1
            """,
            (symbol, timeframe, date),
        )
        r = await cur.fetchone()
        if not r:
            return None
        return (
            int(r[0]),
            float(r[1] or 0.0),
            float(r[2] or 0.0),
            float(r[3] or 0.0),
            float(r[4] or 0.0),
            float(r[5] or 0.0),
            float(r[6] or 0.0),
        )
    finally:
        await conn.close()

async def indicator_exists(symbol: str, timeframe: str, date: int) -> bool:
    row = await fetch_one(
        "indicators.db",
        """
        SELECT 1
        FROM indicators
        WHERE symbol=? AND timeframe=? AND date=?
        LIMIT 1
        """,
        (symbol, timeframe, int(date)),
    )
    return row is not None
