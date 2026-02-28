from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import aiosqlite

from app.config import load_settings
from app.timeutil import now_utc_s


@dataclass(frozen=True)
class Position:
    id: int
    signal_key: str
    symbol: str
    timeframe: str
    signal_date: int
    signal_created_at: int
    signal_type: str
    side: str
    entry: float
    sl: float
    tp: float
    opened_at: int
    status: str
    closed_at: int | None
    close_reason: str | None
    close_price: float | None


async def connect() -> aiosqlite.Connection:
    settings = load_settings(require_keys=False)
    conn = await aiosqlite.connect(settings.trade_manager_db)
    conn.row_factory = aiosqlite.Row
    await conn.execute("PRAGMA journal_mode=WAL;")
    await conn.execute("PRAGMA synchronous=NORMAL;")
    await conn.execute("PRAGMA foreign_keys=ON;")
    return conn


async def ensure_schema(conn: aiosqlite.Connection) -> None:
    await conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS virtual_positions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          signal_key TEXT NOT NULL UNIQUE,
          symbol TEXT NOT NULL,
          timeframe TEXT NOT NULL,
          signal_date INTEGER NOT NULL,
          signal_created_at INTEGER NOT NULL,
          signal_type TEXT NOT NULL,
          side TEXT NOT NULL,
          entry REAL NOT NULL,
          sl REAL NOT NULL,
          tp REAL NOT NULL,
          opened_at INTEGER NOT NULL,
          status TEXT NOT NULL,
          closed_at INTEGER,
          close_reason TEXT,
          close_price REAL,
          hit_source TEXT,
          last_tick_ts INTEGER,
          last_tick_price REAL,
          meta_json TEXT,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS position_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          pos_id INTEGER NOT NULL,
          ts INTEGER NOT NULL,
          event_type TEXT NOT NULL,
          price REAL,
          bid REAL,
          ask REAL,
          payload_json TEXT,
          error TEXT,
          FOREIGN KEY (pos_id) REFERENCES virtual_positions(id)
        );

        CREATE TABLE IF NOT EXISTS manager_state (
          k TEXT PRIMARY KEY,
          v TEXT NOT NULL,
          updated_at INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_vp_status_symbol
          ON virtual_positions(status, symbol);
        CREATE INDEX IF NOT EXISTS idx_vp_close_reason
          ON virtual_positions(close_reason);
        CREATE INDEX IF NOT EXISTS idx_vp_opened_at
          ON virtual_positions(opened_at);
        CREATE INDEX IF NOT EXISTS idx_events_ts
          ON position_events(ts);
        """
    )
    await conn.commit()


async def init_db() -> None:
    conn = await connect()
    try:
        await ensure_schema(conn)
    finally:
        await conn.close()


def build_signal_key(
    symbol: str,
    timeframe: str,
    signal_date: int,
    signal_type: str,
    side: str,
) -> str:
    return f"{symbol}:{timeframe}:{int(signal_date)}:{signal_type}:{side}"


async def get_cursor(conn: aiosqlite.Connection, key: str = "last_seen_signal_id") -> int:
    row = await (
        await conn.execute("SELECT v FROM manager_state WHERE k=?", (key,))
    ).fetchone()
    return int(row["v"]) if row else 0


async def set_cursor(conn: aiosqlite.Connection, value: int, key: str = "last_seen_signal_id") -> None:
    now = now_utc_s()
    await conn.execute(
        """
        INSERT INTO manager_state(k, v, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(k) DO UPDATE SET
          v=excluded.v,
          updated_at=excluded.updated_at
        """,
        (key, str(int(value)), now),
    )


async def fetch_new_signals(
    conn_signals: aiosqlite.Connection,
    after_id: int,
    limit: int,
) -> list[aiosqlite.Row]:
    cur = await conn_signals.execute(
        """
        SELECT id, symbol, timeframe, date, signal_type, side, entry, stop, tp, created_at
        FROM signals
        WHERE id > ?
        ORDER BY id ASC
        LIMIT ?
        """,
        (int(after_id), int(limit)),
    )
    return await cur.fetchall()


async def has_open_position_for_symbol(conn: aiosqlite.Connection, symbol: str) -> bool:
    row = await (
        await conn.execute(
            "SELECT 1 FROM virtual_positions WHERE symbol=? AND status='OPEN' LIMIT 1",
            (symbol,),
        )
    ).fetchone()
    return row is not None


async def insert_virtual_position_from_signal(
    conn: aiosqlite.Connection,
    signal: aiosqlite.Row,
    meta: dict[str, Any] | None = None,
) -> bool:
    now = now_utc_s()
    raw_side = str(signal["side"]).upper()
    side = "LONG" if raw_side in {"LONG", "BUY"} else "SHORT" if raw_side in {"SHORT", "SELL"} else raw_side
    signal_key = build_signal_key(
        signal["symbol"],
        signal["timeframe"],
        signal["date"],
        signal["signal_type"],
        side,
    )

    cur = await conn.execute(
        """
        INSERT OR IGNORE INTO virtual_positions(
          signal_key, symbol, timeframe, signal_date, signal_created_at,
          signal_type, side, entry, sl, tp,
          opened_at, status,
          created_at, updated_at, meta_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', ?, ?, ?)
        """,
        (
            signal_key,
            signal["symbol"],
            signal["timeframe"],
            int(signal["date"]),
            int(signal["created_at"]),
            signal["signal_type"],
            side,
            float(signal["entry"]),
            float(signal["stop"]),
            float(signal["tp"]),
            now,
            now,
            now,
            json.dumps(meta or {}),
        ),
    )
    if cur.rowcount == 1:
        pos_id = cur.lastrowid
        await log_position_event(
            conn,
            pos_id=pos_id,
            event_type="OPENED",
            payload={"signal_id": int(signal["id"]), "signal_key": signal_key},
        )
        return True
    return False


async def log_position_event(
    conn: aiosqlite.Connection,
    pos_id: int,
    event_type: str,
    ts: int | None = None,
    price: float | None = None,
    bid: float | None = None,
    ask: float | None = None,
    payload: dict[str, Any] | None = None,
    error: str | None = None,
) -> None:
    await conn.execute(
        """
        INSERT INTO position_events(pos_id, ts, event_type, price, bid, ask, payload_json, error)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(pos_id),
            int(ts or now_utc_s()),
            event_type,
            price,
            bid,
            ask,
            json.dumps(payload or {}),
            error,
        ),
    )


async def load_open_positions(conn: aiosqlite.Connection) -> list[Position]:
    cur = await conn.execute(
        """
        SELECT
          id, signal_key, symbol, timeframe, signal_date, signal_created_at,
          signal_type, side, entry, sl, tp, opened_at,
          status, closed_at, close_reason, close_price
        FROM virtual_positions
        WHERE status='OPEN'
        ORDER BY opened_at ASC
        """
    )
    rows = await cur.fetchall()
    return [Position(**dict(r)) for r in rows]


async def close_position_atomic(
    conn: aiosqlite.Connection,
    pos_id: int,
    close_reason: str,
    close_price: float,
    hit_source: str,
    tick_ts: int,
) -> bool:
    now = now_utc_s()
    cur = await conn.execute(
        """
        UPDATE virtual_positions
        SET status='CLOSED',
            closed_at=?,
            close_reason=?,
            close_price=?,
            hit_source=?,
            last_tick_ts=?,
            last_tick_price=?,
            updated_at=?
        WHERE id=? AND status='OPEN'
        """,
        (now, close_reason, close_price, hit_source, int(tick_ts), close_price, now, int(pos_id)),
    )
    return cur.rowcount == 1
