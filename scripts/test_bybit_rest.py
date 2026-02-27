import asyncio
from app.bybit.rest import BybitREST

async def main():
    client = BybitREST()

    instruments = await client.get_instruments_linear()
    print("instruments count:", len(instruments))

    tickers = await client.get_tickers_linear()
    print("tickers count:", len(tickers))

    klines = await client.get_kline("BTCUSDT", "240", limit=5)
    print("klines:", klines)

    await client.close()

asyncio.run(main())
