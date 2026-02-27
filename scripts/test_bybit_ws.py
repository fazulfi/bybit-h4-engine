import asyncio
from app.bybit.ws import run_ws_forever

async def main():
    q = asyncio.Queue()

    # Test 1 symbol dulu biar tidak rame
    symbols = ["BTCUSDT"]

    # Run WS as task
    asyncio.create_task(run_ws_forever("240", symbols, q))

    # Print first confirmed candle we receive
    while True:
        candle = await q.get()
        print("CONFIRMED:", candle)
        q.task_done()

asyncio.run(main())
