from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

from app.db.trade_manager import Position


@dataclass
class ManagerState:
    open_positions_by_symbol: dict[str, list[Position]] = field(default_factory=dict)
    last_quote_by_symbol: dict[str, dict[str, Any]] = field(default_factory=dict)
    subscribed_symbols: set[str] = field(default_factory=set)
    symbol_locks: dict[str, asyncio.Lock] = field(default_factory=dict)
    global_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    tick_queue: asyncio.Queue[dict[str, Any]] = field(default_factory=lambda: asyncio.Queue(maxsize=5000))
    dropped_ticks: int = 0
    ws_state: str = "DISCONNECTED"
    last_heartbeat_ts: int = 0
    last_tick_ts: int = 0
    force_reconnect: bool = False

    def get_symbol_lock(self, symbol: str) -> asyncio.Lock:
        lock = self.symbol_locks.get(symbol)
        if lock is None:
            lock = asyncio.Lock()
            self.symbol_locks[symbol] = lock
        return lock

    def desired_subscriptions_unlocked(self) -> set[str]:
        return {s for s, positions in self.open_positions_by_symbol.items() if positions}
