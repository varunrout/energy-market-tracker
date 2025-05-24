#!/usr/bin/env bash
# scripts/run_etl.sh
python - << 'PYCODE'
from src.data_fetch import get_day_ahead_prices, save_prices
df = get_day_ahead_prices()  # Use the wrapper function instead
save_prices(df, path="data/raw")
print(f"Saved {len(df)} rows to data/raw")
PYCODE
