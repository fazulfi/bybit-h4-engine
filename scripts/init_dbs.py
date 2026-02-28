from __future__ import annotations

import sqlite3
from pathlib import Path

from app.config import load_settings

# Catatan:
# - Kita pakai 3 DB terpisah: prices, indicators, signals
# - Semua pakai WAL + synchronous NORMAL (cukup aman + jauh lebih cepat)


def _apply_pragmas(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("PRAGMA temp_store=MEMORY;")
    cur.execute("PRAGMA foreign_keys=ON;")
    conn.commit()


def _exec(db_path: Path, sql: str) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        _apply_pragmas(conn)
        conn.executescript(sql)
        conn.commit()
        print(f"✅ Initialized: {db_path}")
    finally:
        conn.close()


PRICES_SQL = """
CREATE TABLE IF NOT EXISTS prices (
  symbol TEXT NOT NULL,
  timeframe TEXT NOT NULL,
  date INTEGER NOT NULL,             -- candle OPEN time in seconds (UTC)
  open REAL NOT NULL,
  high REAL NOT NULL,
  low REAL NOT NULL,
  close REAL NOT NULL,
  volume REAL NOT NULL,
  PRIMARY KEY(symbol, timeframe, date)
);

CREATE INDEX IF NOT EXISTS idx_prices_symbol_tf_date
  ON prices(symbol, timeframe, date);
"""

INDICATORS_SQL = """
CREATE TABLE IF NOT EXISTS indicators (
  symbol TEXT NOT NULL,
  timeframe TEXT NOT NULL,
  date INTEGER NOT NULL,             -- candle OPEN time in seconds (UTC)

  atr14 REAL,
  atr_pct REAL,

  hh20 REAL,
  ll20 REAL,
  avg_vol20 REAL,
  rvol REAL,

  PRIMARY KEY(symbol, timeframe, date)
);

CREATE INDEX IF NOT EXISTS idx_ind_symbol_tf_date
  ON indicators(symbol, timeframe, date);
"""

SIGNALS_SQL = """
CREATE TABLE IF NOT EXISTS signals (
  id INTEGER PRIMARY KEY AUTOINCREMENT,

  symbol TEXT NOT NULL,
  timeframe TEXT NOT NULL,
  date INTEGER NOT NULL,
  signal_type TEXT NOT NULL,
  side TEXT NOT NULL,

  entry REAL NOT NULL,
  stop REAL NOT NULL,
  tp REAL NOT NULL,

  rvol REAL,
  atr14 REAL,
  atr_pct REAL,
  hh20 REAL,
  ll20 REAL,
  volume REAL,
  close REAL,

  created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_signals_lookup
  ON signals(symbol, timeframe, date, signal_type);

CREATE INDEX IF NOT EXISTS idx_signals_created_at
  ON signals(created_at);
"""


def main() -> None:
    s = load_settings(require_keys=False)

    _exec(s.prices_db, PRICES_SQL)
    _exec(s.indicators_db, INDICATORS_SQL)
    _exec(s.signals_db, SIGNALS_SQL)

    print("✅ All DB schemas created.")


if __name__ == "__main__":
    main()
