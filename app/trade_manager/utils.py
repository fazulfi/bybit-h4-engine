from __future__ import annotations


def normalize_side(side: str) -> str:
    s = side.strip().upper()
    if s in {"BUY", "LONG"}:
        return "LONG"
    if s in {"SELL", "SHORT"}:
        return "SHORT"
    return s
