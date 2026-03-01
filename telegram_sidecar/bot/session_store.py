from __future__ import annotations

import time

MAX_SESSION_AGE_SEC = 6 * 60 * 60
MAX_SESSIONS = 200


def new_session(hash_value: str = "") -> dict[str, object]:
    now = int(time.time())
    return {"hash": hash_value, "created_at": now, "updated_at": now}


def touch_session(session: dict[str, object], *, hash_value: str | None = None) -> None:
    session["updated_at"] = int(time.time())
    if hash_value is not None:
        session["hash"] = hash_value


def cleanup_sessions(sessions: dict[tuple[int, int], dict[str, object]]) -> int:
    now = int(time.time())
    removed = 0

    expired_keys = [
        key
        for key, session in sessions.items()
        if (now - int(session.get("updated_at", session.get("created_at", 0)) or 0)) > MAX_SESSION_AGE_SEC
    ]
    for key in expired_keys:
        sessions.pop(key, None)
        removed += 1

    while len(sessions) > MAX_SESSIONS:
        oldest_key = min(
            sessions,
            key=lambda key: int(
                sessions[key].get("updated_at", sessions[key].get("created_at", 0)) or 0
            ),
        )
        sessions.pop(oldest_key, None)
        removed += 1

    return removed
