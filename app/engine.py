from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

from app.config import load_settings
from app.db.prices import upsert_candle
from app.indicators import compute_for_candle
from app.logger import setup_logger
from app.paper_engine import run_paper_cycle
from app.seed import seed_h4_prices
from app.signals import generate_for_symbol
from app.timeutil import normalize_bybit_ts
from app.universe import build_universe
from app.db.prices import get_last_closed_ts

import time
from datetime import datetime, timezone, timedelta


def seconds_until_next_h4_close() -> int:
    now = datetime.now(timezone.utc)

    hour_block = (now.hour // 4) * 4
    current_close = now.replace(hour=hour_block, minute=0, second=0, microsecond=0)

    if now >= current_close:
        next_close = current_close + timedelta(hours=4)
    else:
        next_close = current_close

    return int((next_close - now).total_seconds())

async def handle_candle(
    candle: Dict[str, Any],
    timeframe: str,
    log,
    dryrun: bool,
) -> None:
    """
    Full pipeline for one confirmed candle:
    save_price -> indicators -> signals -> paper (optional)
    """
    symbol = candle.get("symbol")
    interval = str(candle.get("interval"))

    if not symbol:
        return
    if interval != str(timeframe):
        return

    # Bybit start time in ms -> seconds (UTC)
    open_ts_s = normalize_bybit_ts(candle["start"])

    tf_sec = int(timeframe) * 60
    closed_ts = int(open_ts_s) - tf_sec

    # 1) Save price
    await upsert_candle(
        symbol=symbol,
        timeframe=str(timeframe),
        date=int(open_ts_s),
        o=float(candle["open"]),
        h=float(candle["high"]),
        l=float(candle["low"]),
        c=float(candle["close"]),
        v=float(candle["volume"]),
    )
    log.info(f"PRICE SAVED {symbol} {open_ts_s}")

    # 2) Compute indicators (for last CLOSED candle)
    await compute_for_candle(symbol, str(timeframe), int(closed_ts), log)

    # 3) Generate signal (for last CLOSED candle)
    await generate_for_symbol(symbol, str(timeframe), log, date=int(closed_ts))

    # 4) Paper (optional)
    if dryrun:
        log.info("DRYRUN: skipping paper engine")
        return

    await run_paper_cycle(str(timeframe))


async def run_once(
    timeframe: str,
    log,
    dryrun: bool,
    force_universe_refresh: bool,
    seed_limit: int = 200,
) -> None:
    """
    One-shot pipeline (no websocket):
    - build universe
    - seed historical candles (REST)
    - generate signals for all symbols
    - run paper once (unless dryrun)
    """
    log.info("ONCE mode: building universe...")
    symbols: List[str] = await build_universe(force_refresh=force_universe_refresh)
    log.info(f"Universe size: {len(symbols)}")

    log.info(f"Seeding H4 prices (REST) limit={seed_limit} ...")
    await seed_h4_prices(symbols=symbols, timeframe=str(timeframe), limit=seed_limit, log=log)

    log.info("Generating signals for universe...")
    for sym in symbols:
        try:
            last_ts = await get_last_closed_ts(sym, str(timeframe))
            if not last_ts:
                continue

            await generate_for_symbol(
                sym,
                str(timeframe),
                log,
                date=int(last_ts)
            )
        except Exception as e:
            log.error(f"Signal error {sym}: {e}")

    if dryrun:
        log.info("DRYRUN: skipping paper engine")
        return

    log.info("Running paper cycle...")
    await run_paper_cycle(str(timeframe))
    log.info("ONCE mode complete.")


async def main_engine(
    settings=None,
    timeframe_override: Optional[str] = None,
    log_level_override: Optional[str] = None,
    dryrun: bool = False,
    once: bool = False,
    force_universe_refresh: bool = False,
) -> None:
    """
    Main engine:
    - does NOT mutate settings
    - uses overrides passed from CLI/main.py
    """
    if settings is None:
        settings = load_settings(require_keys=False)

    timeframe = str(timeframe_override or getattr(settings, "timeframe", "240"))
    log_level = str(log_level_override or getattr(settings, "log_level", "INFO"))
    log = setup_logger("engine")

    if once:
        await run_once(
            timeframe=timeframe,
            log=log,
            dryrun=dryrun,
            force_universe_refresh=force_universe_refresh,
        )
        return

    log.info("Building universe...")
    symbols: List[str] = await build_universe(force_refresh=force_universe_refresh)
    log.info(f"Universe size: {len(symbols)}")

    log.info("Engine started. Smart H4 scheduler mode...")

    while True:
        wait_seconds = seconds_until_next_h4_close()

        log.info(f"Sleeping {wait_seconds}s until next H4 close...")
        await asyncio.sleep(wait_seconds + 10)  # buffer 10 detik

        try:
            log.info("H4 closed. Updating candles...")

            for sym in symbols:

    # 1) update price
                await seed_h4_prices(
                    symbols=[sym],
                    timeframe=str(timeframe),
                    limit=2,
                    log=log,
                )

    # 2) ambil candle terakhir
                last_ts = await get_last_closed_ts(sym, str(timeframe))
                if not last_ts:
                    continue

                await compute_for_candle(sym, str(timeframe), int(last_ts), log)
                await generate_for_symbol(sym, str(timeframe), log, date=int(last_ts))

            if not dryrun:
                log.info("Running paper cycle...")
                await run_paper_cycle(str(timeframe))

            log.info("Cycle complete.")

        except Exception as e:
            log.error(f"H4 cycle error: {e}")

if __name__ == "__main__":
    asyncio.run(main_engine())
