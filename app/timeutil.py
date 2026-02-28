from __future__ import annotations

from datetime import datetime, timezone


def ms_to_s(ms: int) -> int:
    """Convert milliseconds to seconds (int)."""
    return int(ms // 1000)


def s_to_ms(s: int) -> int:
    """Convert seconds to milliseconds (int)."""
    return int(s * 1000)


def now_utc_s() -> int:
    """Current UTC time in seconds."""
    return int(datetime.now(timezone.utc).timestamp())


def ts_to_utc_str(ts_s: int) -> str:
    """Seconds timestamp -> UTC string."""
    return datetime.fromtimestamp(ts_s, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def normalize_bybit_ts(ts: int) -> int:
    """
    Bybit kadang kasih timestamp ms (13 digits). Kita normalisasi jadi seconds.
    Kalau sudah seconds (10 digits-ish), biarin.
    """
    # Simple heuristic: ms timestamps are usually >= 1e12
    if ts >= 1_000_000_000_000:
        return ms_to_s(ts)
    return int(ts)


def candle_open_ts(candle: dict) -> int:
    """
    Ambil start/open time candle dari payload Bybit kline.
    Field yang sering muncul: start, startTime, openTime
    """
    for key in ("start", "startTime", "openTime", "t"):
        if key in candle:
            return normalize_bybit_ts(int(candle[key]))
    raise KeyError("Candle open/start timestamp field not found")


def candle_close_ts(candle: dict, timeframe_s: int) -> int:
    """
    Hitung close time = open time + timeframe.
    Lebih konsisten daripada percaya field closeTime yang kadang beda format.
    """
    open_s = candle_open_ts(candle)
    return int(open_s + timeframe_s)


def timeframe_to_seconds(timeframe: str | int) -> int:
    """Convert timeframe minutes (e.g. 240) into seconds."""
    return int(timeframe) * 60


def candle_close_from_open(open_ts_s: int, timeframe: str | int) -> int:
    """Given candle OPEN timestamp, return candle CLOSE timestamp (seconds UTC)."""
    return int(open_ts_s) + timeframe_to_seconds(timeframe)


def candle_open_from_close(close_ts_s: int, timeframe: str | int) -> int:
    """Given candle CLOSE timestamp, return candle OPEN timestamp (seconds UTC)."""
    return int(close_ts_s) - timeframe_to_seconds(timeframe)
