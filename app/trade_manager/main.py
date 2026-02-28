from __future__ import annotations

import argparse
import asyncio

from app.db.trade_manager import init_db
from app.logger import setup_logger
from app.trade_manager.lifecycle import run_trade_manager


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Bybit Trade Manager (virtual positions)")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="Run one ingest iteration")
    mode.add_argument("--daemon", action="store_true", help="Run all loops continuously")
    return p


async def _run(args) -> None:
    log = setup_logger("trade_manager")
    await init_db()

    once = bool(args.once)
    if not args.once and not args.daemon:
        once = True

    await run_trade_manager(once=once, log=log)


def main() -> None:
    args = build_parser().parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
