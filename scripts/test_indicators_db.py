import asyncio
from app.db.indicators import upsert_indicator, get_latest_indicator, has_indicator

async def main():
    sym = "TESTUSDT"
    tf = "240"
    date = 1700000000

    vals = {
        "atr14": 12.34,
        "atr_pct": 0.0123,
        "hh20": 200.0,
        "ll20": 150.0,
        "avg_vol20": 12345.0,
        "rvol": 1.8,
    }

    await upsert_indicator(sym, tf, date, vals)

    print("has:", await has_indicator(sym, tf, date))
    print("latest:", await get_latest_indicator(sym, tf))

asyncio.run(main())
