from __future__ import annotations

import argparse
import asyncio

from app.config import load_settings
from app.engine import main_engine


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Bybit H4 Engine (signals + paper)")
    p.add_argument("--timeframe", type=str, default=None, help="e.g. 240 for H4")
    p.add_argument("--dryrun", action="store_true", help="Skip paper engine")
    p.add_argument("--once", action="store_true", help="Run once (seed+signals+paper) without WS")
    p.add_argument("--force-universe-refresh", action="store_true", help="Rebuild universe cache")
    p.add_argument("--log-level", type=str, default=None, help="INFO/DEBUG/WARNING/ERROR")
    return p


async def _run(args) -> None:
    settings = load_settings(require_keys=False)

    timeframe = args.timeframe or getattr(settings, "timeframe", "240")
    log_level = args.log_level or getattr(settings, "log_level", "INFO")

    await main_engine(
        settings=settings,
        timeframe_override=str(timeframe),
        log_level_override=str(log_level),
        dryrun=bool(args.dryrun),
        once=bool(args.once),
        force_universe_refresh=bool(args.force_universe_refresh),
    )


def main() -> None:
    args = build_parser().parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
