from app.timeutil import normalize_bybit_ts, ts_to_utc_str, candle_close_ts

# contoh timestamp Bybit ms
ms = 1700000000000
s = normalize_bybit_ts(ms)
print("ms->s:", ms, "->", s, ts_to_utc_str(s))

# contoh candle dummy
candle = {"start": 1700000000000}  # ms
close_s = candle_close_ts(candle, timeframe_s=240 * 60)
print("close:", close_s, ts_to_utc_str(close_s))
