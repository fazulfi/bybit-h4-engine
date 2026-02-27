import asyncio
from app.universe import build_universe

async def main():
    symbols = await build_universe(force_refresh=True)
    print("Universe size:", len(symbols))
    print("First 10:", symbols[:10])

asyncio.run(main())
