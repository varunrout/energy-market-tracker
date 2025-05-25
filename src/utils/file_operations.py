import pandas as pd
from datetime import datetime
import os
from typing import Optional
from .. import config

def save_prices(df: pd.DataFrame, path: Optional[str] = None) -> None:
    """Saves DataFrame to a CSV file named with the current date."""
    save_path = path if path is not None else config.DEFAULT_SAVE_PATH
    
    if df.empty:
        print("DataFrame is empty. Nothing to save.")
        return

    # Ensure the directory exists
    os.makedirs(save_path, exist_ok=True)
    
    file_date = datetime.utcnow().strftime('%Y%m%d')
    output_filename = f"prices_{file_date}.csv"
    full_path = os.path.join(save_path, output_filename)
    
    try:
        df.to_csv(full_path, index=False)
        print(f"Successfully saved data to {full_path}")
    except Exception as e:
        print(f"Error saving data to {full_path}: {e}")

