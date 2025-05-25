import pandas as pd
from datetime import datetime, timedelta
import random
from .. import config

def generate_mock_price_data(date: datetime = None) -> pd.DataFrame:
    """Generate mock electricity price data for testing."""
    date = date or datetime.utcnow()
    day_str = date.strftime("%Y%m%d")
    
    records = []
    for i in range(24): # Generate 24 hourly records
        record_date = pd.to_datetime(day_str, format="%Y%m%d") + pd.to_timedelta(i, unit="h")
        price = random.uniform(config.MOCK_PRICE_MIN, config.MOCK_PRICE_MAX)
        records.append({"date": record_date, "price_€/MWh": price})
        
    mock_df = pd.DataFrame(records)
    return mock_df[["date", "price_€/MWh"]]
