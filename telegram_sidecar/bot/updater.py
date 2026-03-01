from __future__ import annotations

import asyncio

from telegram import Bot

from telegram_sidecar.api.services import fetch_engine_snapshot
from telegram_sidecar.config import load_settings
from telegram_sidecar.formatter.engine import format_engine
from telegram_sidecar.bot.hashutil import compute_hash

sessions: dict[tuple[int, int], dict[str, str]] = {}


async def auto_update(bot: Bot) -> None:
    settings = load_settings()
    interval = int(settings["POLL_INTERVAL_SEC"])

    while True:
        for key, session in list(sessions.items()):
            chat_id, message_id = key
            try:
                vm = await fetch_engine_snapshot()
                text = format_engine(vm)
                content_hash = compute_hash(text)
                if content_hash != session.get("hash"):
                    await bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=message_id,
                        text=text,
                        parse_mode="HTML",
                    )
                    session["hash"] = content_hash
            except Exception:
                continue
        await asyncio.sleep(interval)
