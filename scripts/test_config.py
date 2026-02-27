from app.config import load_settings

s = load_settings(require_keys=False)
print("timeframe:", s.timeframe)
print("db_dir:", s.db_dir)
print("prices_db:", s.prices_db)
