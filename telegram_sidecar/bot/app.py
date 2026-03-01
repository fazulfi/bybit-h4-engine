from __future__ import annotations

import asyncio
import logging

from telegram.ext import ApplicationBuilder, CallbackQueryHandler, CommandHandler

from telegram_sidecar.bot.router import handle_callback, start
from telegram_sidecar.bot.updater import auto_update
from telegram_sidecar.config import load_settings

logger = logging.getLogger(__name__)


async def run_bot() -> None:
    settings = load_settings()
    token = str(settings.get("BOT_TOKEN") or "")
    if not token:
        logger.error("BOT_TOKEN missing; bot will not start")
        return

    app = ApplicationBuilder().token(token).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(handle_callback))

    asyncio.create_task(auto_update(app.bot))
    await app.run_polling()
