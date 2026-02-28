from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# Load .env from project root
ROOT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(ROOT_DIR / ".env")


def _env(key: str, default: str | None = None) -> str:
    val = os.getenv(key, default)
    if val is None or val == "":
        raise RuntimeError(f"Missing required env var: {key}")
    return val


def _env_int(key: str, default: int) -> int:
    raw = os.getenv(key, str(default))
    try:
        return int(raw)
    except ValueError as e:
        raise RuntimeError(f"Invalid int for {key}: {raw}") from e


def _env_float(key: str, default: float) -> float:
    raw = os.getenv(key, str(default))
    try:
        return float(raw)
    except ValueError as e:
        raise RuntimeError(f"Invalid float for {key}: {raw}") from e


@dataclass(frozen=True)
class Settings:
    # Bybit
    bybit_api_key: str
    bybit_api_secret: str
    category: str
    timeframe: str  # keep as string for subjects, e.g. "240"

    # Filters / limits
    min_turnover_24h: float
    max_symbols: int

    # Paths
    root_dir: Path
    db_dir: Path
    prices_db: Path
    indicators_db: Path
    signals_db: Path
    trade_manager_db: Path
    logs_dir: Path


def load_settings(require_keys: bool = False) -> Settings:
    """
    require_keys=False:
      allow empty BYBIT_API_KEY/SECRET for early stages (public endpoints only).
    require_keys=True:
      enforce key/secret exist (for private endpoints).
    """
    root_dir = ROOT_DIR
    db_dir = root_dir / "database"
    logs_dir = root_dir / "logs"

    db_dir.mkdir(exist_ok=True)
    logs_dir.mkdir(exist_ok=True)

    api_key = os.getenv("BYBIT_API_KEY", "")
    api_secret = os.getenv("BYBIT_API_SECRET", "")
    if require_keys and (not api_key or not api_secret):
        raise RuntimeError("BYBIT_API_KEY/BYBIT_API_SECRET required but missing")

    category = os.getenv("CATEGORY", "linear")
    timeframe = os.getenv("TIMEFRAME", "240")

    return Settings(
        bybit_api_key=api_key,
        bybit_api_secret=api_secret,
        category=category,
        timeframe=timeframe,
        min_turnover_24h=_env_float("MIN_TURNOVER_24H", 5_000_000),
        max_symbols=_env_int("MAX_SYMBOLS", 300),
        root_dir=root_dir,
        db_dir=db_dir,
        prices_db=db_dir / "prices.db",
        indicators_db=db_dir / "indicators.db",
        signals_db=db_dir / "signals.db",
        trade_manager_db=db_dir / "trade_manager.db",
        logs_dir=logs_dir,
    )
