from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class TradeManagerConfig:
    ingest_batch_size: int = 500
    ingest_poll_sec: float = 1.0
    health_log_sec: int = 60
    liveness_timeout_sec: int = 45
    trigger_price_mode: str = "bidask"  # bidask | last_price
    max_open_per_symbol: int = 1
    ws_url: str = "wss://stream.bybit.com/v5/public/linear"


def load_trade_manager_config() -> TradeManagerConfig:
    return TradeManagerConfig(
        ingest_batch_size=int(os.getenv("TM_INGEST_BATCH_SIZE", "500")),
        ingest_poll_sec=float(os.getenv("TM_INGEST_POLL_SEC", "1.0")),
        health_log_sec=int(os.getenv("TM_HEALTH_LOG_SEC", "60")),
        liveness_timeout_sec=int(os.getenv("TM_LIVENESS_TIMEOUT_SEC", "45")),
        trigger_price_mode=os.getenv("TM_HIT_PRICE_MODE", "bidask"),
        max_open_per_symbol=int(os.getenv("TM_MAX_OPEN_PER_SYMBOL", "1")),
        ws_url=os.getenv("TM_WS_URL", "wss://stream.bybit.com/v5/public/linear"),
    )
