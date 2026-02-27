import asyncio
from app.db.signals import insert_signal_if_new, get_new_signals, update_signal_status

async def main():
    sym = "TESTUSDT"
    tf = "240"
    date = 1700000000

    inserted = await insert_signal_if_new(
        symbol=sym,
        timeframe=tf,
        date=date,
        signal_type="BREAKOUT_T2",
        side="LONG",
        entry=100,
        stop=90,
        tp=120,
        extra={"rvol": 2.1},
    )
    print("inserted:", inserted)

    # Try insert again (should be False)
    inserted2 = await insert_signal_if_new(
        symbol=sym,
        timeframe=tf,
        date=date,
        signal_type="BREAKOUT_T2",
        side="LONG",
        entry=100,
        stop=90,
        tp=120,
    )
    print("inserted_again:", inserted2)

    news = await get_new_signals()
    print("new_signals:", news)

    await update_signal_status(sym, tf, date, "BREAKOUT_T2", "PAPER_OPENED")
    news_after = await get_new_signals()
    print("new_after_update:", news_after)

asyncio.run(main())
