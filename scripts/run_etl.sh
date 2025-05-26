#!/usr/bin/env bash
# scripts/run_etl.sh
python - << 'PYCODE'
from src.fetching.price_fetchers import get_day_ahead_prices # Corrected: directly use the smart wrapper
from src.utils.file_operations import save_prices
from src import config # Import config to potentially access DEFAULT_SAVE_PATH if needed directly

# Fetch data and the source name
df, source_name = get_day_ahead_prices()  # Use the wrapper function

if not df.empty:
    df['source'] = source_name  # Add the source name as a column
    save_prices(df, path=config.DEFAULT_SAVE_PATH) # Use configured path
    print(f"Saved {len(df)} rows from source '{source_name}' to {config.DEFAULT_SAVE_PATH}")
else:
    print(f"No data fetched (source attempt: '{source_name}'). Nothing saved.")

PYCODE
