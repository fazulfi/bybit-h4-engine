from __future__ import annotations

import asyncio
from typing import Optional
from app.db.paper import has_open_position

from app.config import load_settings
from app.db.paper import (
    open_position,
    close_position,
    get_open_positions,
    snapshot_equity,
    signal_event_exists,
    insert_signal_event
)
from app.db.signals import get_recent_signals
from app.db.prices import get_latest_candle
from app.logger import setup_logger
from app.timeutil import now_utc_s

async def process_new_signals(timeframe: str, log) -> None:
    """
    Open paper position for NEW signals.
    Entry at latest candle OPEN (approx next open simulation).
    """
    settings = load_settings(require_keys=False)
    signals = await get_recent_signals(limit=100)
    log.info(f"NEW_SIGNALS={len(signals)}")

    for sig in signals:
        symbol, tf, date, signal_type, side, entry, stop, tp, status, created_at = sig

        if await signal_event_exists(symbol, tf, date, signal_type):
            log.info(f"SKIP_DUPLICATE {symbol} tf={tf} date={date} signal={signal_type}")
            continue

        if await has_open_position(symbol):
            await insert_signal_event(symbol, tf, date, signal_type, "BLOCKED")
            await update_signal_status(symbol, tf, date, signal_type, "BLOCKED")
            log.info(f"BLOCKED {symbol} signal={signal_type} (already has OPEN)")
            continue

# gunakan timeframe dari sinyal (tf), bukan argumen CLI
        candle = await get_latest_candle(symbol, tf)
        if not candle:
            await insert_signal_event(symbol, tf, date, signal_type, "NO_CANDLE")
            await update_signal_status(symbol, tf, date, signal_type, "NO_CANDLE")
            log.warning(f"NO_CANDLE {symbol} tf={tf} signal={signal_type}")
            continue

        open_time, o, h, l, close, volume = candle

        # simulate slippage
        slip = settings.slippage

        if side == "LONG":
            entry_price = o * (1 + slip)
        else:
            entry_price = o * (1 - slip)

        # fixed notional sizing
        notional = settings.paper_notional
        qty = notional / entry_price
        leverage = 10.0

        try:
            pos_id = await open_position(
                symbol=symbol,
                timeframe=tf,
                side=side,
                entry_time=open_time,
                entry_price=entry_price,
                qty=qty,
                leverage=leverage,
                stop_price=stop,
                tp_price=tp,
            )
        except Exception as e:
            await insert_signal_event(symbol, tf, date, signal_type, "OPEN_FAILED", note=str(e))
            log.exception(f"OPEN_FAILED {symbol} tf={tf} signal={signal_type}")
            await update_signal_status(symbol, tf, date, signal_type, "ERROR")
            continue

        if not pos_id:
            await insert_signal_event(symbol, tf, date, signal_type, "OPEN_FAILED", note="no pos_id")
            log.error(f"OPEN_FAILED_NO_ID {symbol} tf={tf} signal={signal_type}")
            await update_signal_status(symbol, tf, date, signal_type, "ERROR")
            continue

        await insert_signal_event(symbol, tf, date, signal_type, "OPENED", position_id=pos_id)
        await update_signal_status(symbol, tf, date, signal_type, "PAPER_OPENED")
        log.info(f"PAPER OPEN {symbol} id={pos_id} tf={tf} entry={entry_price}")


async def update_open_positions(timeframe: str, log) -> None:
    """
    Check SL/TP hit using latest candle high/low.
    Conservative rule: if both hit → SL first.
    """
    settings = load_settings(require_keys=False)
    positions = await get_open_positions()

    for pos in positions:
        (
            pos_id,
            symbol,
            tf,
            side,
            entry_time,
            entry_price,
            qty,
            leverage,
            stop_price,
            tp_price,
            status,
            exit_time,
            exit_price,
            exit_reason,
            pnl,
            pnl_pct,
        ) = pos

        candle = await get_latest_candle(symbol, tf)
        if not candle:
            continue

        ts, o, high, low, close, volume = candle

        hit_sl = False
        hit_tp = False

        if side == "LONG":
            if low <= stop_price:
                hit_sl = True
            if high >= tp_price:
                hit_tp = True
        else:
            if high >= stop_price:
                hit_sl = True
            if low <= tp_price:
                hit_tp = True

        if not hit_sl and not hit_tp:
            continue

        # Conservative: SL first if both hit
        if hit_sl:
            exit_price = stop_price
            reason = "SL"
        else:
            exit_price = tp_price
            reason = "TP"

        # Apply slippage on exit
        slip = settings.slippage
        if side == "LONG":
            exit_price = exit_price * (1 - slip)
        else:
            exit_price = exit_price * (1 + slip)

        await close_position(
            position_id=pos_id,
            exit_time=ts,
            exit_price=exit_price,
            exit_reason=reason,
            fee_rate=settings.fee_rate,
        )

        log.info(f"PAPER CLOSE {symbol} id={pos_id} reason={reason}")


async def run_paper_cycle(timeframe: str) -> None:
    log = setup_logger("paper")

    await process_new_signals(timeframe, log)
    await update_open_positions(timeframe, log)

    # Equity snapshot placeholder (upgrade later)
    await snapshot_equity(
        time=now_utc_s(),
        equity=0.0,
        balance=0.0,
        unrealized=0.0,
    )

    log.info("Paper cycle done.")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Paper trading engine")
    parser.add_argument("--timeframe", type=str, default=None)

    args = parser.parse_args()

    settings = load_settings(require_keys=False)
    timeframe = args.timeframe or settings.timeframe

    asyncio.run(run_paper_cycle(timeframe))

    print("PAPER_DB=", settings.paper_db)

if __name__ == "__main__":
    main()
