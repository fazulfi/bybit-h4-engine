import asyncio

from app.db.signals import get_recent_signals, insert_signal


async def main():
    sym = "TESTUSDT"
    tf = "240"
    date = 1700000000

    inserted = await insert_signal(
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

    # Try insert again (duplicates are allowed in raw signals)
    inserted2 = await insert_signal(
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

    recent = await get_recent_signals(limit=20)
    print("recent_signals:", recent)


asyncio.run(main())
