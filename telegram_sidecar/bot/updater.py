from __future__ import annotations

import asyncio
import logging

from telegram import Bot

from telegram_sidecar.api.services import fetch_engine_snapshot
from telegram_sidecar.config import load_settings
from telegram_sidecar.formatter.engine import format_engine
from telegram_sidecar.bot.hashutil import compute_hash
from telegram_sidecar.models.viewmodels import EngineViewModel
from telegram_sidecar.bot.pollutil import safe_poll_interval
from telegram_sidecar.bot.session_store import cleanup_sessions, touch_session

# NOTE: Volatile in-memory session store. Sessions are reset on process restart.
sessions: dict[tuple[int, int], dict[str, object]] = {}
logger = logging.getLogger(__name__)



async def _fetch_engine_vm_with_fallback() -> EngineViewModel:
    try:
        return await fetch_engine_snapshot()
    except Exception:
        return EngineViewModel(
            state="DISCONNECTED",
            ws="DISCONNECTED",
            heartbeat_ms=0.0,
            dropped_5m=0,
            open_positions=0,
            stale=True,
        )


async def auto_update(bot: Bot) -> None:
    settings = load_settings()
    interval = safe_poll_interval(settings.get("POLL_INTERVAL_SEC"))

    while True:
        try:
            removed = cleanup_sessions(sessions)
            if removed:
                logger.debug("session cleanup removed=%s remaining=%s", removed, len(sessions))
        except Exception:
            logger.exception("session cleanup failed")

        if not sessions:
            await asyncio.sleep(interval)
            continue

        vm = await _fetch_engine_vm_with_fallback()
        text = format_engine(vm)
        content_hash = compute_hash(text)

        for (chat_id, message_id), session in list(sessions.items()):
            try:
                if content_hash == session.get("hash"):
                    touch_session(session)
                    continue

                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=text,
                    parse_mode="HTML",
                )
                touch_session(session, hash_value=content_hash)
            except Exception as e:
                msg = str(e).lower()
                fatal = (
                    "message to edit not found" in msg
                    or "message can't be edited" in msg
                    or "chat not found" in msg
                    or "forbidden" in msg
                    or "bot was blocked" in msg
                )

                logger.exception(
                    "auto_update failed for chat_id=%s message_id=%s fatal=%s",
                    chat_id,
                    message_id,
                    fatal,
                )

                if fatal:
                    sessions.pop((chat_id, message_id), None)
        await asyncio.sleep(interval)
