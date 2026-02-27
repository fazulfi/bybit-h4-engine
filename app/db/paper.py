from __future__ import annotations

from typing import List, Optional, Tuple

import aiosqlite

from app.config import load_settings


# Row type for paper_positions
PaperPositionRow = Tuple[
    int,    # id
    str,    # symbol
    str,    # timeframe
    str,    # side
    int,    # entry_time
    float,  # entry_price
    float,  # qty
    float,  # leverage
    float,  # stop_price
    float,  # tp_price
    str,    # status
    int | None,  # exit_time
    float | None, # exit_price
    str | None,   # exit_reason
    float | None, # pnl
    float | None  # pnl_pct
]


async def _connect() -> aiosqlite.Connection:
    s = load_settings(require_keys=False)
    conn = await aiosqlite.connect(s.paper_db)
    await conn.execute("PRAGMA journal_mode=WAL;")
    await conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


# =========================
# OPEN POSITION
# =========================

async def open_position(
    symbol: str,
    timeframe: str,
    side: str,  # LONG / SHORT
    entry_time: int,
    entry_price: float,
    qty: float,
    leverage: float,
    stop_price: float,
    tp_price: float,
) -> int:
    """
    Insert new OPEN position.
    Return position_id.
    """
    sql = """
    INSERT INTO paper_positions(
        symbol, timeframe, side,
        entry_time, entry_price,
        qty, leverage,
        stop_price, tp_price,
        status
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN')
    """

    conn = await _connect()
    try:
        cur = await conn.execute(
            sql,
            (
                symbol,
                timeframe,
                side,
                entry_time,
                float(entry_price),
                float(qty),
                float(leverage),
                float(stop_price),
                float(tp_price),
            ),
        )
        await conn.commit()
        return cur.lastrowid
    finally:
        await conn.close()


# =========================
# GET OPEN POSITIONS
# =========================

async def get_open_positions(symbol: Optional[str] = None) -> List[PaperPositionRow]:
    conn = await _connect()
    try:
        if symbol:
            cur = await conn.execute(
                "SELECT * FROM paper_positions WHERE status='OPEN' AND symbol=?",
                (symbol,),
            )
        else:
            cur = await conn.execute(
                "SELECT * FROM paper_positions WHERE status='OPEN'"
            )

        rows = await cur.fetchall()
        return rows
    finally:
        await conn.close()


# =========================
# CLOSE POSITION
# =========================

async def close_position(
    exit_time: int,
    exit_price: float,
    exit_reason: str,
    fee_rate: float,
    position_id: int,
) -> None:
    """
    Close position and compute PnL.
    PnL linear USDT:
        LONG:  (exit - entry) * qty
        SHORT: (entry - exit) * qty
    Fee applied on notional (entry + exit).
    """
    conn = await _connect()
    try:
        cur = await conn.execute(
            "SELECT side, entry_price, qty FROM paper_positions WHERE id=?",
            (position_id,),
        )
        row = await cur.fetchone()
        if not row:
            return

        side, entry_price, qty = row
        entry_price = float(entry_price)
        qty = float(qty)
        exit_price = float(exit_price)

        if side == "LONG":
            gross_pnl = (exit_price - entry_price) * qty
        else:
            gross_pnl = (entry_price - exit_price) * qty

        # fee applied twice: entry + exit
        notional_entry = entry_price * qty
        notional_exit = exit_price * qty
        total_fee = (notional_entry + notional_exit) * fee_rate

        net_pnl = gross_pnl - total_fee
        pnl_pct = net_pnl / notional_entry if notional_entry > 0 else 0.0

        await conn.execute(
            """
            UPDATE paper_positions
            SET status='CLOSED',
                exit_time=?,
                exit_price=?,
                exit_reason=?,
                pnl=?,
                pnl_pct=?
            WHERE id=?
            """,
            (
                exit_time,
                exit_price,
                exit_reason,
                net_pnl,
                pnl_pct,
                position_id,
            ),
        )
        await conn.commit()
    finally:
        await conn.close()


# =========================
# EQUITY SNAPSHOT
# =========================

async def snapshot_equity(
    time: int,
    equity: float,
    balance: float,
    unrealized: float,
) -> None:
    """
    Save equity snapshot per candle close.
    """
    conn = await _connect()
    try:
        await conn.execute(
            """
            INSERT OR REPLACE INTO paper_equity(time, equity, balance, unrealized)
            VALUES (?, ?, ?, ?)
            """,
            (time, float(equity), float(balance), float(unrealized)),
        )
        await conn.commit()
    finally:
        await conn.close()

# =======================
# BLOCK POSITION IF OPEN
# =======================

async def has_open_position(symbol: str) -> bool:
    conn = await _connect()
    try:
        cur = await conn.execute(
            "SELECT 1 FROM paper_positions WHERE status='OPEN' AND symbol=? LIMIT 1",
            (symbol,),
        )
        return (await cur.fetchone()) is not None
    finally:
        await conn.close()


# =====================================
# SIGNAL EVENT TRACKING (NEW)
# =====================================

async def signal_event_exists(
    symbol: str,
    timeframe: str,
    date: int,
    signal_type: str,
) -> bool:
    """
    Check if paper already processed this signal.
    """
    conn = await _connect()
    try:
        cur = await conn.execute(
            """
            SELECT 1 FROM paper_signal_events
            WHERE symbol=? AND timeframe=? AND date=? AND signal_type=?
              AND action IN ('OPENED','BLOCKED','NO_CANDLE','OPEN_FAILED')
            LIMIT 1
            """,
            (symbol, timeframe, date, signal_type),
        )
        return (await cur.fetchone()) is not None
    finally:
        await conn.close()


async def insert_signal_event(
    symbol: str,
    timeframe: str,
    date: int,
    signal_type: str,
    action: str,
    position_id: Optional[int] = None,
    note: Optional[str] = None,
) -> None:
    from app.timeutil import now_utc_s

    conn = await _connect()
    try:
        await conn.execute(
            """
            INSERT OR IGNORE INTO paper_signal_events(
              symbol, timeframe, date, signal_type,
              action, position_id, note, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                symbol,
                timeframe,
                date,
                signal_type,
                action,
                position_id,
                note,
                now_utc_s(),
            ),
        )
        await conn.commit()
    finally:
        await conn.close()
