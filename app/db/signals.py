from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import aiosqlite

from app.config import load_settings
from app.timeutil import now_utc_s


# Row:
# (symbol, timeframe, date, signal_type, side, entry, stop, tp, created_at)
SignalRow = Tuple[str, str, int, str, str, float, float, float, int]


async def _ensure_signals_schema(conn: aiosqlite.Connection) -> None:
    """
    Ensure signals table is raw append-only (duplicates allowed).
    """
    cur = await conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='signals'"
    )
    row = await cur.fetchone()
    ddl = (row[0] or "") if row else ""

    # Old schema has composite PK that blocks duplicate raw signals.
    needs_migration = "PRIMARY KEY(symbol, timeframe, date, signal_type)" in ddl

    if not needs_migration:
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_signals_lookup
            ON signals(symbol, timeframe, date, signal_type)
            """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_signals_created_at
            ON signals(created_at)
            """
        )
        await conn.commit()
        return

    await conn.execute("BEGIN")
    try:
        await conn.execute("ALTER TABLE signals RENAME TO signals_old")
        await conn.execute(
            """
            CREATE TABLE signals (
              id INTEGER PRIMARY KEY AUTOINCREMENT,

              symbol TEXT NOT NULL,
              timeframe TEXT NOT NULL,
              date INTEGER NOT NULL,
              signal_type TEXT NOT NULL,
              side TEXT NOT NULL,

              entry REAL NOT NULL,
              stop REAL NOT NULL,
              tp REAL NOT NULL,

              rvol REAL,
              atr14 REAL,
              atr_pct REAL,
              hh20 REAL,
              ll20 REAL,
              volume REAL,
              close REAL,

              created_at INTEGER NOT NULL
            )
            """
        )
        await conn.execute(
            """
            INSERT INTO signals(
                symbol, timeframe, date,
                signal_type, side,
                entry, stop, tp,
                rvol, atr14, atr_pct, hh20, ll20, volume, close,
                created_at
            )
            SELECT
                symbol, timeframe, date,
                signal_type, side,
                entry, stop, tp,
                rvol, atr14, atr_pct, hh20, ll20, volume, close,
                created_at
            FROM signals_old
            """
        )
        await conn.execute("DROP TABLE signals_old")
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_signals_lookup
            ON signals(symbol, timeframe, date, signal_type)
            """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_signals_created_at
            ON signals(created_at)
            """
        )
        await conn.commit()
    except Exception:
        await conn.rollback()
        raise


async def _connect() -> aiosqlite.Connection:
    s = load_settings(require_keys=False)
    conn = await aiosqlite.connect(s.signals_db)
    await conn.execute("PRAGMA journal_mode=WAL;")
    await conn.execute("PRAGMA synchronous=NORMAL;")
    await _ensure_signals_schema(conn)
    return conn


async def insert_signal(
    symbol: str,
    timeframe: str,
    date: int,
    signal_type: str,
    side: str,
    entry: float,
    stop: float,
    tp: float,
    extra: Optional[Dict[str, float]] = None,
) -> bool:
    """
    Insert raw signal row (duplicates allowed by design).
    Return True if inserted.
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
    """

    conn = await _connect()
    try:
        cur = await conn.execute(
            sql,
            (
                symbol,
                timeframe,
                int(date),
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
        return cur.rowcount == 1
    finally:
        await conn.close()


async def insert_signal_if_new(
    symbol: str,
    timeframe: str,
    date: int,
    signal_type: str,
    side: str,
    entry: float,
    stop: float,
    tp: float,
    extra: Optional[Dict[str, float]] = None,
) -> bool:
    """Backward-compatible alias; duplicates are still allowed."""
    return await insert_signal(
        symbol=symbol,
        timeframe=timeframe,
        date=date,
        signal_type=signal_type,
        side=side,
        entry=entry,
        stop=stop,
        tp=tp,
        extra=extra,
    )


async def get_recent_signals(limit: int = 200) -> List[SignalRow]:
    """Get recent signals ordered by created_at DESC."""
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
            ORDER BY created_at DESC, id DESC
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
                created_at
            FROM signals
            WHERE symbol=? AND timeframe=? AND date=? AND signal_type=?
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (symbol, timeframe, int(date), signal_type),
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
            int(r[8]),
        )
    finally:
        await conn.close()
