from __future__ import annotations

import json
from pathlib import Path
from typing import List

from app.bybit.rest import BybitREST
from app.config import load_settings
from app.logger import setup_logger


CACHE_FILE = "universe.json"


async def build_universe(force_refresh: bool = False) -> List[str]:
    """
    Build list of USDT linear perpetual symbols
    filtered by 24h turnover and limited by MAX_SYMBOLS.
    """

    log = setup_logger("universe")
    settings = load_settings(require_keys=False)

    cache_path: Path = settings.db_dir / CACHE_FILE

    # If cache exists and not forced → load
    if cache_path.exists() and not force_refresh:
        log.info("Loading universe from cache")
        with open(cache_path, "r") as f:
            return json.load(f)

    client = BybitREST()

    try:
        log.info("Fetching instruments...")
        instruments = await client.get_instruments_linear()

        # Filter only USDT perpetual (settleCoin = USDT)
        linear_usdt = [
            i for i in instruments
            if i.get("settleCoin") == "USDT"
            and i.get("status") == "Trading"
        ]

        log.info(f"Total linear USDT instruments: {len(linear_usdt)}")

        log.info("Fetching tickers...")
        tickers = await client.get_tickers_linear()

        # Map symbol -> turnover24h
        turnover_map = {}
        for t in tickers:
            sym = t.get("symbol")
            turnover = float(t.get("turnover24h", 0))
            turnover_map[sym] = turnover

        # Attach turnover & filter
        filtered = []
        for inst in linear_usdt:
            sym = inst["symbol"]
            turnover = turnover_map.get(sym, 0)

            if turnover >= settings.min_turnover_24h:
                filtered.append((sym, turnover))

        # Sort by turnover descending
        filtered.sort(key=lambda x: x[1], reverse=True)

        # Apply max limit
        limited = filtered[: settings.max_symbols]

        symbols = [sym for sym, _ in limited]

        log.info(f"Universe size after filter: {len(symbols)}")

        # Save cache
        with open(cache_path, "w") as f:
            json.dump(symbols, f)

        log.info(f"Universe cached at {cache_path}")

        return symbols

    finally:
        await client.close()
