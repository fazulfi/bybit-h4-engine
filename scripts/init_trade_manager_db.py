from __future__ import annotations

import sys
from pathlib import Path
# Allow running as: python scripts/<file>.py
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import asyncio

from app.db.trade_manager import init_db


def main() -> None:
    asyncio.run(init_db())
    print("✅ Initialized trade_manager.db")


if __name__ == "__main__":
    main()
