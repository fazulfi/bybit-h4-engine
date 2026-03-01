from __future__ import annotations

from telegram import Update
from telegram.ext import ContextTypes

from telegram_sidecar.api.services import fetch_engine_snapshot
from telegram_sidecar.bot.callbacks import parse_callback
from telegram_sidecar.bot.keyboard import home_keyboard
from telegram_sidecar.bot.hashutil import compute_hash
from telegram_sidecar.bot.updater import sessions
from telegram_sidecar.config import load_settings
from telegram_sidecar.formatter.engine import format_engine


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings = load_settings()
    allowed_user_ids = settings["ALLOWED_USER_IDS"]
    user_id = update.effective_user.id if update.effective_user else 0

    if allowed_user_ids and user_id not in allowed_user_ids:
        await update.message.reply_text("unauthorized")
        return

    message = await update.message.reply_text("Menu:", reply_markup=home_keyboard())
    sessions[(update.effective_chat.id, message.message_id)] = {"hash": ""}


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    view, _params = parse_callback(query.data or "")
    if view != "engine":
        await query.edit_message_text("Menu:", reply_markup=home_keyboard())
        return

    vm = await fetch_engine_snapshot()
    text = format_engine(vm)
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=home_keyboard())
    sessions[(query.message.chat_id, query.message.message_id)] = {"hash": compute_hash(text)}
