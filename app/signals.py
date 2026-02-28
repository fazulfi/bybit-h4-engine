from __future__ import annotations

import argparse
import asyncio
from typing import Optional

from app.config import load_settings
from app.db.indicators import get_indicator, get_latest_indicator
from app.db.prices import get_candle, get_latest_candle
from app.db.signals import insert_signal
from app.logger import setup_logger


MIN_RVOL = 2.1
MIN_ATR_PCT = 0.01
RR_MULTIPLIER = 2.0


async def generate_for_symbol(symbol: str, timeframe: str, log, date: int | None = None):
    """
    Generate breakout signal for a specific candle OPEN timestamp (recommended: last_closed_open).
    If date is None, fallback to latest (not ideal for scheduler).
    """

    # 1) Ambil indicator & candle untuk date tertentu
    if date is not None:
        ind = await get_indicator(symbol, timeframe, int(date))
        if not ind:
            # Optional log biar gak silent
            # log.info(f"No indicator {symbol} {timeframe} @ {date}")
            return

        candle = await get_candle(symbol, timeframe, int(date))
        if not candle:
            # log.info(f"No candle {symbol} {timeframe} @ {date}")
            return

    else:
        # Fallback lama (buat debug manual)
        ind = await get_latest_indicator(symbol, timeframe)
        if not ind:
            return
        candle = await get_latest_candle(symbol, timeframe)
        if not candle:
            return

    # Unpack indicator
    ind_date, atr14, atr_pct, hh20, ll20, avg_vol20, rvol = ind

    # Unpack candle
    c_date, o, h, l, close, volume = candle

    # 2) Pastikan cocok (kalau date dikirim, ini harusnya selalu match)
    if c_date != ind_date:
        # log.info(f"Skip mismatch {symbol} {timeframe} candle={c_date} ind={ind_date}")
        return

    # ======================
    # LONG Breakout
    # ======================
    if close > hh20 and rvol >= MIN_RVOL and atr_pct >= MIN_ATR_PCT:
        entry = close
        stop = hh20
        risk = entry - stop
        if risk <= 0:
            return
        tp = entry + (risk * RR_MULTIPLIER)

        inserted = await insert_signal(
            symbol=symbol,
            timeframe=timeframe,
            date=c_date,
            signal_type="BREAKOUT_LONG",
            side="LONG",
            entry=entry,
            stop=stop,
            tp=tp,
            extra={
                "rvol": rvol,
                "atr14": atr14,
                "atr_pct": atr_pct,
                "hh20": hh20,
                "ll20": ll20,
                "volume": volume,
                "close": close,
            },
        )

        if inserted:
            log.info(
                f"SIGNAL LONG {symbol} @ {c_date} "
                f"entry={entry:.4f} stop={stop:.4f} tp={tp:.4f}"
            )

    # ======================
    # SHORT Breakout
    # ======================
    if close < ll20 and rvol >= MIN_RVOL and atr_pct >= MIN_ATR_PCT:
        entry = close
        stop = ll20
        risk = stop - entry
        if risk <= 0:
            return
        tp = entry - (risk * RR_MULTIPLIER)

        inserted = await insert_signal(
            symbol=symbol,
            timeframe=timeframe,
            date=c_date,
            signal_type="BREAKOUT_SHORT",
            side="SHORT",
            entry=entry,
            stop=stop,
            tp=tp,
            extra={
                "rvol": rvol,
                "atr14": atr14,
                "atr_pct": atr_pct,
                "hh20": hh20,
                "ll20": ll20,
                "volume": volume,
                "close": close,
            },
        )

        if inserted:
            log.info(
                f"SIGNAL SHORT {symbol} @ {c_date} "
                f"entry={entry:.4f} stop={stop:.4f} tp={tp:.4f}"
            )

async def run_signal_scan(timeframe: str) -> None:
    from app.universe import build_universe

    log = setup_logger("signals")
    symbols = await build_universe(force_refresh=False)

    for sym in symbols:
        try:
            await generate_for_symbol(sym, timeframe, log)
        except Exception as e:
            log.error(f"Signal error {sym}: {e}")

    log.info("Signal scan completed.")


def main():
    parser = argparse.ArgumentParser(description="Signal generator")
    parser.add_argument("--timeframe", type=str, default=None)

    args = parser.parse_args()

    settings = load_settings(require_keys=False)
    timeframe = args.timeframe or settings.timeframe

    asyncio.run(run_signal_scan(timeframe))


if __name__ == "__main__":
    main()
