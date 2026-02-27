import asyncio
from app.db.paper import open_position, get_open_positions, close_position

async def main():
    pos_id = await open_position(
        symbol="TESTUSDT",
        timeframe="240",
        side="LONG",
        entry_time=1700000000,
        entry_price=100,
        qty=1,
        leverage=1,
        stop_price=90,
        tp_price=120,
    )

    print("opened id:", pos_id)

    open_positions = await get_open_positions()
    print("open_positions:", open_positions)

    await close_position(
        position_id=pos_id,
        exit_time=1700003600,
        exit_price=110,
        exit_reason="TP",
        fee_rate=0.0006,
    )

    open_positions_after = await get_open_positions()
    print("open_positions_after:", open_positions_after)

asyncio.run(main())
