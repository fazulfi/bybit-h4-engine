import asyncio
from app.db.prices import upsert_candle, get_last_ts, get_latest_candle, get_window_metrics_prev20

async def main():
    sym = "TESTUSDT"
    tf = "240"

    # Insert 21 candle dummy (biar prev20 ada)
    base = 1700000000
    for i in range(21):
        ts = base + i * 240 * 60
        await upsert_candle(sym, tf, ts, 1+i, 2+i, 0.5+i, 1.5+i, 100+i)

    last_ts = await get_last_ts(sym, tf)
    latest = await get_latest_candle(sym, tf)
    metrics = await get_window_metrics_prev20(sym, tf, last_ts)

    print("last_ts:", last_ts)
    print("latest:", latest)
    print("metrics:", metrics)

asyncio.run(main())
