from __future__ import annotations

import argparse
import asyncio
from typing import List

from app.bybit.rest import BybitREST
from app.config import load_settings
from app.db.prices import upsert_candle
from app.logger import setup_logger
from app.timeutil import normalize_bybit_ts
from app.universe import build_universe


async def seed_symbol(
    client: BybitREST,
    symbol: str,
    timeframe: str,
    limit: int,
    log,
) -> None:
    klines = await client.get_kline(symbol, timeframe, limit=limit)

    for k in klines:
        open_ts_s = normalize_bybit_ts(k["start"])  # ms → seconds

        await upsert_candle(
            symbol=symbol,
            timeframe=timeframe,
            date=open_ts_s,
            o=k["open"],
            h=k["high"],
            l=k["low"],
            c=k["close"],
            v=k["volume"],
        )

    log.info(f"Seeded {symbol}: {len(klines)} candles")


async def run_seed(timeframe: str, limit: int, max_symbols: int | None) -> None:
    log = setup_logger("seed")
    settings = load_settings(require_keys=False)

    log.info("Building universe...")
    symbols: List[str] = await build_universe(force_refresh=False)

    if max_symbols:
        symbols = symbols[:max_symbols]

    log.info(f"Seeding {len(symbols)} symbols...")

    client = BybitREST()

    try:
        for idx, sym in enumerate(symbols, start=1):
            try:
                await seed_symbol(client, sym, timeframe, limit, log)
            except Exception as e:
                log.error(f"Seed error {sym}: {e}")

            # sedikit delay supaya sopan ke API
            await asyncio.sleep(0.05)

            if idx % 20 == 0:
                log.info(f"Progress: {idx}/{len(symbols)}")

    finally:
        await client.close()

    log.info("Seeding completed.")

async def seed_h4_prices(
    symbols: List[str],
    timeframe: str,
    limit: int,
    log,
) -> None:
    client = BybitREST()
    try:
        for idx, sym in enumerate(symbols, start=1):
            try:
                await seed_symbol(client, sym, timeframe, limit, log)
            except Exception as e:
                log.error(f"Seed error {sym}: {e}")
            await asyncio.sleep(0.05)
            if idx % 20 == 0:
                log.info(f"Seed progress: {idx}/{len(symbols)}")
    finally:
        await client.close()

def main():
    parser = argparse.ArgumentParser(description="Seed H4 historical data")
    parser.add_argument("--timeframe", type=str, default=None)
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--max-symbols", type=int, default=None)

    args = parser.parse_args()

    settings = load_settings(require_keys=False)
    timeframe = args.timeframe or settings.timeframe

    asyncio.run(run_seed(timeframe, args.limit, args.max_symbols))


if __name__ == "__main__":
    main()
