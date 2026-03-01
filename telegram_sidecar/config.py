from __future__ import annotations

import os
from functools import lru_cache

from dotenv import load_dotenv


def _parse_allowed_ids(raw: str | None) -> set[int]:
    if not raw:
        return set()
    result: set[int] = set()
    for part in raw.split(","):
        value = part.strip()
        if not value:
            continue
        try:
            result.add(int(value))
        except ValueError:
            continue
    return result


@lru_cache(maxsize=1)
def load_settings() -> dict[str, object]:
    load_dotenv("/etc/bybit-sidecar/env")

    return {
        "BOT_TOKEN": os.getenv("BOT_TOKEN", ""),
        "ALLOWED_USER_IDS": _parse_allowed_ids(os.getenv("ALLOWED_USER_IDS")),
        "DB_PATH": os.getenv("DB_PATH", "data/signals.db"),
        "ENGINE_STATE_URL": os.getenv("ENGINE_STATE_URL", "http://127.0.0.1:9101/internal/state"),
        "SIDECAR_BIND": os.getenv("SIDECAR_BIND", "127.0.0.1"),
        "SIDECAR_PORT": int(os.getenv("SIDECAR_PORT", "9111")),
        "POLL_INTERVAL_SEC": int(os.getenv("POLL_INTERVAL_SEC", "5")),
    }
