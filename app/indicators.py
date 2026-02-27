from __future__ import annotations

import argparse
import asyncio
from typing import Dict, List, Optional

from app.config import load_settings
from app.db.prices import (
    get_recent_candles_upto,
    get_window_metrics_prev20,
)
from app.db.indicators import upsert_indicator, has_indicator
from app.logger import setup_logger


def compute_atr14(candles: List[tuple]) -> Optional[float]:
    """
    candles: list of (date, open, high, low, close, volume)
    Must contain at least 15 candles (so 14 TR values using prev close).
    We compute simple SMA ATR (not Wilder smoothing) for clarity.
    """
    if len(candles) < 15:
        return None

    trs = []

    for i in range(1, 15):
        _, _, high, low, close, _ = candles[i]
        _, _, _, _, prev_close, _ = candles[i - 1]

        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close),
        )
        trs.append(tr)

    return sum(trs) / len(trs)


async def compute_for_candle(
    symbol: str,
    timeframe: str,
    current_date: int,  # candle OPEN time (seconds UTC)
    log,
) -> None:
    """
    Compute indicators for specific candle.
    """


    # 1️⃣ Get window metrics from SQL (prev 20 candles)
    window = await get_window_metrics_prev20(symbol, timeframe, current_date)
    if not window:
        return

    # 2️⃣ Get recent candles (need 15 for ATR + current candle)
    candles = await get_recent_candles_upto(
        symbol,
        timeframe,
        current_date,
        20
    )
    if not candles or len(candles) < 15:
        return

    # 3️⃣ Compute ATR14
    atr14 = compute_atr14(candles[-15:])
    if atr14 is None:
        return

    _, _, _, _, close, volume = candles[-1]

    avg_vol20 = window["avg_vol20"]
    rvol = volume / avg_vol20 if avg_vol20 > 0 else 0.0
    atr_pct = atr14 / close if close > 0 else 0.0

    values: Dict[str, float] = {
        "atr14": atr14,
        "atr_pct": atr_pct,
        "hh20": window["hh20"],
        "ll20": window["ll20"],
        "avg_vol20": avg_vol20,
        "rvol": rvol,
    }

    await upsert_indicator(symbol, timeframe, current_date, values)

    log.info(
        f"{symbol} {timeframe} @ {current_date} | "
        f"ATR={atr14:.4f} RVOL={rvol:.2f}"
    )


# =========================================
# CLI precompute mode
# =========================================

async def precompute_all(timeframe: str) -> None:
    """
    Loop through all prices and compute indicators for all eligible candles.
    """

    from app.universe import build_universe
    from app.db.prices import get_all_dates
    from app.db.indicators import indicator_exists

    log = setup_logger("indicators-precompute")
    settings = load_settings(require_keys=False)

    symbols = await build_universe(force_refresh=False)

    for sym in symbols:
        try:
            dates = await get_all_dates(sym, timeframe)
            if not dates or len(dates) < 20:
                continue

            for current_date in dates:
                # skip if already computed
                if await indicator_exists(sym, timeframe, current_date):
                    continue

                try:
                    await compute_for_candle(sym, timeframe, current_date, log)
                except Exception as e:
                    log.error(f"Indicator error {sym} @ {current_date}: {e}")

        except Exception as e:
            log.error(f"Precompute error {sym}: {e}")

    log.info("Precompute done.")


def main():
    parser = argparse.ArgumentParser(description="Indicator computation")
    parser.add_argument("--precompute", action="store_true")
    parser.add_argument("--timeframe", type=str, default=None)

    args = parser.parse_args()

    settings = load_settings(require_keys=False)
    timeframe = args.timeframe or settings.timeframe

    if args.precompute:
        asyncio.run(precompute_all(timeframe))


if __name__ == "__main__":
    main()
