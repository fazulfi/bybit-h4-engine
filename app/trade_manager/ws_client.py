from __future__ import annotations

import asyncio
import json
import time

from app.metrics_tm import tm_dropped_ticks_total, tm_exceptions_total, tm_reconnect_total
from app.trade_manager.router import on_tick
from app.trade_manager.state import ManagerState


def _topic_for(symbol: str) -> str:
    return f"tickers.{symbol}"


def _normalize_ts(ts: int) -> int:
    if ts > 1_000_000_000_000:
        return int(ts // 1000)
    return int(ts)


def _parse_ticker(msg: dict) -> list[dict]:
    topic = msg.get("topic", "")
    if not topic.startswith("tickers."):
        return []

    payload = msg.get("data")
    if isinstance(payload, dict):
        payload = [payload]
    if not isinstance(payload, list):
        return []

    out: list[dict] = []
    for item in payload:
        symbol = item.get("symbol")
        if not symbol:
            continue

        raw_ts = int(item.get("time") or item.get("timestamp") or msg.get("ts") or int(time.time()))
        out.append(
            {
                "symbol": symbol,
                "ts": _normalize_ts(raw_ts),
                "last": float(item["lastPrice"]) if item.get("lastPrice") else None,
                "bid": float(item["bid1Price"]) if item.get("bid1Price") else None,
                "ask": float(item["ask1Price"]) if item.get("ask1Price") else None,
            }
        )
    return out


async def _sync_subscriptions(ws, state: ManagerState) -> None:
    async with state.global_lock:
        desired = state.desired_subscriptions_unlocked()
        subscribed = set(state.subscribed_symbols)

    new_symbols = desired - subscribed
    old_symbols = subscribed - desired

    if new_symbols:
        await ws.send(
            json.dumps({"op": "subscribe", "args": [_topic_for(s) for s in sorted(new_symbols)]})
        )
        async with state.global_lock:
            state.subscribed_symbols |= new_symbols

    if old_symbols:
        await ws.send(
            json.dumps({"op": "unsubscribe", "args": [_topic_for(s) for s in sorted(old_symbols)]})
        )
        async with state.global_lock:
            state.subscribed_symbols -= old_symbols


async def tick_worker_loop(state: ManagerState, hit_mode: str, log) -> None:
    while True:
        tick = await state.tick_queue.get()
        try:
            await on_tick(state, tick["symbol"], tick, hit_mode)
        except Exception as exc:
            tm_exceptions_total.inc()
            log.warning("TM tick worker error: %s", exc)
        finally:
            state.tick_queue.task_done()


async def ws_loop(state: ManagerState, ws_url: str, hit_mode: str, log) -> None:
    backoffs = [1, 2, 5, 10, 30]
    attempt = 0

    while True:
        async with state.global_lock:
            state.ws_state = "CONNECTING"
        try:
            import websockets

            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20) as ws:
                async with state.global_lock:
                    state.ws_state = "CONNECTED"
                    state.subscribed_symbols.clear()
                    state.last_heartbeat_ts = int(time.time())
                    state.force_reconnect = False
                attempt = 0

                while True:
                    await _sync_subscriptions(ws, state)

                    async with state.global_lock:
                        force_reconnect = state.force_reconnect
                    if force_reconnect:
                        tm_reconnect_total.inc()
                        log.warning("TM WS force reconnect triggered by liveness checker")
                        break

                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    except asyncio.TimeoutError:
                        continue

                    now = int(time.time())
                    async with state.global_lock:
                        state.last_heartbeat_ts = now

                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        log.warning("TM WS received invalid JSON frame")
                        continue

                    for quote in _parse_ticker(msg):
                        symbol = quote["symbol"]
                        async with state.global_lock:
                            state.last_quote_by_symbol[symbol] = quote
                            state.last_tick_ts = int(quote["ts"])

                        try:
                            state.tick_queue.put_nowait(quote)
                        except asyncio.QueueFull:
                            tm_dropped_ticks_total.inc()
                            async with state.global_lock:
                                state.dropped_ticks += 1

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            tm_exceptions_total.inc()
            tm_reconnect_total.inc()
            async with state.global_lock:
                state.ws_state = "DISCONNECTED"
                state.subscribed_symbols.clear()
            delay = backoffs[min(attempt, len(backoffs) - 1)]
            attempt += 1
            log.warning("TM WS disconnected (%s), retry in %ss", exc, delay)
            await asyncio.sleep(delay)
        else:
            async with state.global_lock:
                state.ws_state = "DISCONNECTED"
                state.subscribed_symbols.clear()
                state.force_reconnect = False
            # closed intentionally (e.g. force reconnect)
            await asyncio.sleep(0)
