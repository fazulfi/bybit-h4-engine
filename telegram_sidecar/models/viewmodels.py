from __future__ import annotations

from dataclasses import dataclass


@dataclass
class EngineViewModel:
    state: str
    ws: str
    heartbeat_ms: float
    dropped_5m: int
    open_positions: int
    stale: bool = False
