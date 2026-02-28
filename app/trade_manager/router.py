from __future__ import annotations

from app.db.trade_manager import close_position_atomic, connect, log_position_event
from app.trade_manager.evaluator import evaluate_hit
from app.trade_manager.state import ManagerState


async def on_tick(state: ManagerState, symbol: str, quote: dict, hit_mode: str) -> None:
    lock = state.get_symbol_lock(symbol)
    async with lock:
        async with state.global_lock:
            positions = list(state.open_positions_by_symbol.get(symbol, []))
        if not positions:
            return

        conn = await connect()
        changed = False
        try:
            for pos in positions:
                result = evaluate_hit(pos, quote, hit_mode)
                if not result.should_close:
                    continue

                ok = await close_position_atomic(
                    conn,
                    pos_id=pos.id,
                    close_reason=str(result.close_reason),
                    close_price=float(result.close_price),
                    hit_source=str(result.hit_source),
                    tick_ts=int(quote.get("ts") or 0),
                )
                if not ok:
                    continue

                changed = True
                await log_position_event(
                    conn,
                    pos_id=pos.id,
                    event_type="CLOSED",
                    price=result.close_price,
                    bid=quote.get("bid"),
                    ask=quote.get("ask"),
                    payload={"reason": result.close_reason, "source": result.hit_source},
                )

                async with state.global_lock:
                    state.open_positions_by_symbol[symbol] = [
                        p for p in state.open_positions_by_symbol.get(symbol, []) if p.id != pos.id
                    ]

            if changed:
                await conn.commit()
        finally:
            await conn.close()
