from __future__ import annotations

from dataclasses import dataclass

from app.db.trade_manager import Position


@dataclass(frozen=True)
class EvalResult:
    should_close: bool
    close_reason: str | None = None
    close_price: float | None = None
    hit_source: str | None = None


def evaluate_hit(
    position: Position,
    quote: dict,
    mode: str,
) -> EvalResult:
    side = position.side.upper()
    bid = quote.get("bid")
    ask = quote.get("ask")
    last = quote.get("last")

    if mode == "bidask" and (bid is None or ask is None):
        return EvalResult(should_close=False)

    if mode == "bidask":
        long_price = float(bid)
        short_price = float(ask)
        source = "bidask"
    else:
        if last is None:
            return EvalResult(should_close=False)
        long_price = float(last)
        short_price = float(last)
        source = "last_price"

    if side == "LONG":
        if long_price <= position.sl:
            return EvalResult(True, "SL", long_price, source)
        if long_price >= position.tp:
            return EvalResult(True, "TP", long_price, source)
    elif side == "SHORT":
        if short_price >= position.sl:
            return EvalResult(True, "SL", short_price, source)
        if short_price <= position.tp:
            return EvalResult(True, "TP", short_price, source)

    return EvalResult(should_close=False)
