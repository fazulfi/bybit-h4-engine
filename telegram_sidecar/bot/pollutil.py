from __future__ import annotations


def safe_poll_interval(raw_interval: object) -> int:
    try:
        parsed = int(raw_interval)
    except (TypeError, ValueError):
        return 5
    return max(1, parsed)
