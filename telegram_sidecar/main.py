from __future__ import annotations

import asyncio

import uvicorn

from telegram_sidecar.api.app import app as api_app
from telegram_sidecar.bot.app import run_bot
from telegram_sidecar.config import load_settings


async def main() -> None:
    settings = load_settings()
    server = uvicorn.Server(
        uvicorn.Config(
            api_app,
            host=str(settings["SIDECAR_BIND"]),
            port=int(settings["SIDECAR_PORT"]),
            log_level="info",
        )
    )

    await asyncio.gather(server.serve(), run_bot())


if __name__ == "__main__":
    asyncio.run(main())
