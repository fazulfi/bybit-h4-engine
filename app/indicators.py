from __future__ import annotations

import argparse
import asyncio
from typing import Dict, List, Optional

import aiosqlite

from app.config import load_settings
from app.db.prices import (
    _connect as prices_connect,
    get_all_dates_with_conn,
    get_recent_candles_upto,
    get_recent_candles_upto_with_conn,
    get_window_metrics_prev20,
    get_window_metrics_prev20_with_conn,
)
from app.db.indicators import (
    _connect as indicators_connect,
    get_all_dates_with_conn as get_indicator_dates_with_conn,
    upsert_indicator,
    upsert_indicators_bulk,
)
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
    prices_conn: Optional[aiosqlite.Connection] = None,
    indicators_conn: Optional[aiosqlite.Connection] = None,
) -> None:
    """
    Compute indicators for specific candle.
    """


    # 1️⃣ Get window metrics from SQL (prev 20 candles)
    if prices_conn is None:
        window = await get_window_metrics_prev20(symbol, timeframe, current_date)
    else:
        window = await get_window_metrics_prev20_with_conn(prices_conn, symbol, timeframe, current_date)
    if not window:
        return

    # 2️⃣ Get recent candles (need 15 for ATR + current candle)
    if prices_conn is None:
        candles = await get_recent_candles_upto(
            symbol,
            timeframe,
            current_date,
            20,
        )
    else:
        candles = await get_recent_candles_upto_with_conn(
            prices_conn,
            symbol,
            timeframe,
            current_date,
            20,
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

    await upsert_indicator(
        symbol,
        timeframe,
        current_date,
        values,
        conn=indicators_conn,
        commit=indicators_conn is None,
    )

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
    log = setup_logger("indicators-precompute")
    symbols = await build_universe(force_refresh=False)

    for sym in symbols:
        try:
            prices_conn = await prices_connect()
            indicators_conn = await indicators_connect()
            try:
                dates = await get_all_dates_with_conn(prices_conn, sym, timeframe)
                if not dates or len(dates) < 20:
                    continue

                existing_indicator_dates = set(
                    await get_indicator_dates_with_conn(indicators_conn, sym, timeframe)
                )

                pending_rows: List[tuple[str, str, int, Dict[str, float]]] = []
                for current_date in dates:
                    # skip if already computed
                    if current_date in existing_indicator_dates:
                        continue

                    try:
                        values = await _compute_values_for_candle(
                            sym,
                            timeframe,
                            current_date,
                            prices_conn,
                        )
                        if not values:
                            continue
                        pending_rows.append((sym, timeframe, current_date, values))
                        existing_indicator_dates.add(current_date)

                        if len(pending_rows) >= 200:
                            count = await upsert_indicators_bulk(
                                pending_rows,
                                conn=indicators_conn,
                                commit=True,
                            )
                            pending_rows.clear()
                            log.info(f"{sym} {timeframe} batch upsert={count}")
                    except Exception as e:
                        log.error(f"Indicator error {sym} @ {current_date}: {e}")

                if pending_rows:
                    count = await upsert_indicators_bulk(
                        pending_rows,
                        conn=indicators_conn,
                        commit=True,
                    )
                    log.info(f"{sym} {timeframe} final batch upsert={count}")
            finally:
                await prices_conn.close()
                await indicators_conn.close()

        except Exception as e:
            log.error(f"Precompute error {sym}: {e}")

    log.info("Precompute done.")


async def _compute_values_for_candle(
    symbol: str,
    timeframe: str,
    current_date: int,
    prices_conn: aiosqlite.Connection,
) -> Optional[Dict[str, float]]:
    window = await get_window_metrics_prev20_with_conn(prices_conn, symbol, timeframe, current_date)
    if not window:
        return None

    candles = await get_recent_candles_upto_with_conn(
        prices_conn,
        symbol,
        timeframe,
        current_date,
        20,
    )
    if not candles or len(candles) < 15:
        return None

    atr14 = compute_atr14(candles[-15:])
    if atr14 is None:
        return None

    _, _, _, _, close, volume = candles[-1]
    avg_vol20 = window["avg_vol20"]
    rvol = volume / avg_vol20 if avg_vol20 > 0 else 0.0
    atr_pct = atr14 / close if close > 0 else 0.0

    return {
        "atr14": atr14,
        "atr_pct": atr_pct,
        "hh20": window["hh20"],
        "ll20": window["ll20"],
        "avg_vol20": avg_vol20,
        "rvol": rvol,
    }


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
