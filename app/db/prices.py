from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import aiosqlite

from app.config import load_settings

from app.db._db import fetch_all

# Row tuple: (date, open, high, low, close, volume)
CandleRow = Tuple[int, float, float, float, float, float]


async def _connect() -> aiosqlite.Connection:
    s = load_settings(require_keys=False)
    conn = await aiosqlite.connect(s.prices_db)
    # Pragmas for performance (WAL already set in init, but harmless)
    await conn.execute("PRAGMA journal_mode=WAL;")
    await conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


async def upsert_candle(
    symbol: str,
    timeframe: str,
    date: int,  # candle OPEN time (seconds UTC)
    o: float,
    h: float,
    l: float,
    c: float,
    v: float,
) -> None:
    """
    Insert candle (idempotent). If exists, replace values.
    """
    sql = """
    INSERT INTO prices(symbol, timeframe, date, open, high, low, close, volume)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(symbol, timeframe, date) DO UPDATE SET
      open=excluded.open,
      high=excluded.high,
      low=excluded.low,
      close=excluded.close,
      volume=excluded.volume
    """
    conn = await _connect()
    try:
        await conn.execute(sql, (symbol, timeframe, date, o, h, l, c, v))
        await conn.commit()
    finally:
        await conn.close()


async def upsert_candles_bulk(
    rows: List[tuple[str, str, int, float, float, float, float, float]],
    conn: Optional[aiosqlite.Connection] = None,
    commit: bool = True,
) -> int:
    """
    Bulk upsert candles. Rows are:
      (symbol, timeframe, date, open, high, low, close, volume)
    """
    if not rows:
        return 0

    sql = """
    INSERT INTO prices(symbol, timeframe, date, open, high, low, close, volume)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(symbol, timeframe, date) DO UPDATE SET
      open=excluded.open,
      high=excluded.high,
      low=excluded.low,
      close=excluded.close,
      volume=excluded.volume
    """

    owns_conn = conn is None
    if conn is None:
        conn = await _connect()

    try:
        await conn.executemany(sql, rows)
        if commit:
            await conn.commit()
    finally:
        if owns_conn:
            await conn.close()

    return len(rows)


async def get_last_ts(symbol: str, timeframe: str) -> Optional[int]:
    """
    Return latest candle OPEN time (seconds UTC) for symbol+timeframe.
    """
    conn = await _connect()
    try:
        cur = await conn.execute(
            "SELECT MAX(date) FROM prices WHERE symbol=? AND timeframe=?",
            (symbol, timeframe),
        )
        row = await cur.fetchone()
        return int(row[0]) if row and row[0] is not None else None
    finally:
        await conn.close()

async def get_last_closed_open_ts(symbol: str, timeframe: str) -> Optional[int]:
    """
    Return last CLOSED candle OPEN time.
    For H4: last_closed_open = last_open - 14400
    """
    last_open = await get_last_ts(symbol, timeframe)
    if not last_open:
        return None
    tf_sec = int(timeframe) * 60  # timeframe '240' -> 14400
    return int(last_open) - tf_sec


async def get_last_closed_ts(symbol: str, timeframe: str) -> Optional[int]:
    """
    Backward-compatible alias for get_last_closed_open_ts().
    """
    return await get_last_closed_open_ts(symbol, timeframe)


async def get_recent_candles(symbol: str, timeframe: str, limit: int) -> List[CandleRow]:
    """
    Return last N candles ordered ASC by date (oldest->newest).
    """
    conn = await _connect()
    try:
        cur = await conn.execute(
            """
            SELECT date, open, high, low, close, volume
            FROM prices
            WHERE symbol=? AND timeframe=?
            ORDER BY date DESC
            LIMIT ?
            """,
            (symbol, timeframe, limit),
        )
        rows = await cur.fetchall()
        # rows returned newest->oldest, reverse to ASC
        rows = list(reversed(rows))
        return [(int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])) for r in rows]
    finally:
        await conn.close()


async def get_latest_candle(symbol: str, timeframe: str) -> Optional[CandleRow]:
    """
    Return latest candle row (OPEN time).
    """
    conn = await _connect()
    try:
        cur = await conn.execute(
            """
            SELECT date, open, high, low, close, volume
            FROM prices
            WHERE symbol=? AND timeframe=?
            ORDER BY date DESC
            LIMIT 1
            """,
            (symbol, timeframe),
        )
        r = await cur.fetchone()
        if not r:
            return None
        return (int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]))
    finally:
        await conn.close()

async def get_candle(symbol: str, timeframe: str, date: int) -> Optional[CandleRow]:
    """
    Return candle row for exact candle OPEN time (seconds UTC).
    """
    conn = await _connect()
    try:
        cur = await conn.execute(
            """
            SELECT date, open, high, low, close, volume
            FROM prices
            WHERE symbol=? AND timeframe=? AND date=?
            LIMIT 1
            """,
            (symbol, timeframe, int(date)),
        )
        r = await cur.fetchone()
        if not r:
            return None
        return (int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]))
    finally:
        await conn.close()

async def get_window_metrics_prev20(symbol: str, timeframe: str, current_date: int) -> Optional[Dict[str, float]]:
    """
    Metrics untuk breakout: HH20/LL20/avg_vol20 berdasarkan 20 candle SEBELUM current_date.
    Ini penting: exclude candle sekarang supaya tidak lookahead.

    Return dict: {hh20, ll20, avg_vol20}
    """
    conn = await _connect()
    try:
        return await get_window_metrics_prev20_with_conn(conn, symbol, timeframe, current_date)
    finally:
        await conn.close()

async def get_all_dates(symbol: str, timeframe: str) -> list[int]:
    rows = await fetch_all(
        "prices.db",
        """
        SELECT date
        FROM prices
        WHERE symbol=? AND timeframe=?
        ORDER BY date ASC
        """,
        (symbol, timeframe),
    )
    return [int(r[0]) for r in rows]


async def get_all_dates_with_conn(
    conn: aiosqlite.Connection,
    symbol: str,
    timeframe: str,
) -> list[int]:
    cur = await conn.execute(
        """
        SELECT date
        FROM prices
        WHERE symbol=? AND timeframe=?
        ORDER BY date ASC
        """,
        (symbol, timeframe),
    )
    rows = await cur.fetchall()
    return [int(r[0]) for r in rows]

async def get_recent_candles_upto(
    symbol: str,
    timeframe: str,
    end_date: int,
    limit: int
) -> List[CandleRow]:

    conn = await _connect()
    try:
        return await get_recent_candles_upto_with_conn(conn, symbol, timeframe, end_date, limit)
    finally:
        await conn.close()


async def get_window_metrics_prev20_with_conn(
    conn: aiosqlite.Connection,
    symbol: str,
    timeframe: str,
    current_date: int,
) -> Optional[Dict[str, float]]:
    cur = await conn.execute(
        """
        WITH prev AS (
          SELECT high, low, volume
          FROM prices
          WHERE symbol=? AND timeframe=? AND date < ?
          ORDER BY date DESC
          LIMIT 20
        )
        SELECT
          MAX(high) AS hh20,
          MIN(low)  AS ll20,
          AVG(volume) AS avg_vol20
        FROM prev
        """,
        (symbol, timeframe, current_date),
    )
    row = await cur.fetchone()
    if not row or row[0] is None:
        return None
    return {
        "hh20": float(row[0]),
        "ll20": float(row[1]),
        "avg_vol20": float(row[2]),
    }


async def get_recent_candles_upto_with_conn(
    conn: aiosqlite.Connection,
    symbol: str,
    timeframe: str,
    end_date: int,
    limit: int,
) -> List[CandleRow]:
    cur = await conn.execute(
        """
        SELECT date, open, high, low, close, volume
        FROM prices
        WHERE symbol=? AND timeframe=? AND date <= ?
        ORDER BY date DESC
        LIMIT ?
        """,
        (symbol, timeframe, end_date, limit),
    )
    rows = await cur.fetchall()
    rows = list(reversed(rows))
    return [
        (int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]))
        for r in rows
    ]
