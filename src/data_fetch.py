import os, requests, pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import random
import json
from typing import Optional, Dict, List, Any

load_dotenv()

# Get configuration
ENTSOE_API_KEY = os.getenv("ENTSOE_API_KEY")
DATA_SOURCE = os.getenv("DATA_SOURCE", "auto")
# Added new API keys
EIA_API_KEY = os.getenv("EIA_API_KEY")
ELEXON_API_KEY = os.getenv("ELEXON_API_KEY")
NORD_POOL_API_KEY = os.getenv("NORD_POOL_API_KEY")

def fetch_day_ahead_prices(date: datetime = None) -> pd.DataFrame:
    """Pulls 24 h of GB day-ahead prices for given date (UTC)."""
    date = date or datetime.utcnow()
    day = date.strftime("%Y%m%d")
    params = {
        "documentType":"A44","processType":"A01",
        "in_Domain":"10YGB----------A","out_Domain":"10YGB----------A",
        "periodStart": day+"0000","periodEnd": day+"2359",
        "securityToken": ENTSOE_API_KEY
    }
    resp = requests.get("https://transparency.entsoe.eu/api", params=params)
    resp.raise_for_status()
    # parse XML points into DataFrame
    df = pd.read_xml(
        resp.content,
        xpath="//TimeSeries/Period/Point",
        namespaces={"": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:0"}
    )
    df["hour"] = df["position"].astype(int) - 1
    df["date"] = pd.to_datetime(day, format="%Y%m%d") \
                   + pd.to_timedelta(df["hour"], unit="h")
    return df.rename(columns={"price.amount":"price_€/MWh"})[["date","price_€/MWh"]]

def fetch_elexon_prices(date: datetime = None) -> pd.DataFrame:
    """Fetch GB day-ahead prices from Elexon Insights API."""
    if not ELEXON_API_KEY:
        raise ValueError("ELEXON_API_KEY not configured")
        
    date = date or datetime.utcnow()
    
    # Format date for API
    from_date = date.strftime("%Y-%m-%d")
    to_date = (date + timedelta(days=1)).strftime("%Y-%m-%d")
    
    url = f"https://data.elexon.co.uk/bmrs/api/v1/datasets/DayAheadAuction/stream"
    params = {
        "publishDateTimeFrom": f"{from_date}T00:00:00Z",
        "publishDateTimeTo": f"{to_date}T00:00:00Z",
        "format": "json",
    }
    
    headers = {"X-Api-Key": ELEXON_API_KEY}
    
    resp = requests.get(url, params=params, headers=headers)
    resp.raise_for_status()
    
    data = resp.json()
    
    # Create DataFrame from response
    records = []
    for item in data:
        hour = item.get("settlementPeriod", 0) - 1
        price = item.get("price", 0)
        
        records.append({
            "hour": hour,
            "price_€/MWh": price,
            "date": pd.to_datetime(from_date) + pd.to_timedelta(hour, unit="h")
        })
    
    return pd.DataFrame(records)[["date", "price_€/MWh"]]

def fetch_eia_prices(date: datetime = None) -> pd.DataFrame:
    """Fetch US electricity prices from EIA API (uses regional pricing)."""
    if not EIA_API_KEY:
        raise ValueError("EIA_API_KEY not configured")
        
    date = date or datetime.utcnow()
    
    # EIA hourly data series for day-ahead prices (using PJM region as example)
    series_id = "EBA.PJM-ALL.DF.H"
    
    # Format date for API
    start_date = date.strftime("%Y%m%dT00")
    end_date = date.strftime("%Y%m%dT23")
    
    url = f"https://api.eia.gov/series/"
    params = {
        "api_key": EIA_API_KEY,
        "series_id": series_id,
        "start": start_date,
        "end": end_date
    }
    
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    
    data = resp.json()
    
    # Extract price data
    records = []
    if 'series' in data and data['series']:
        series_data = data['series'][0]['data']
        for timestamp, price in series_data:
            dt = pd.to_datetime(timestamp)
            records.append({
                "date": dt,
                "price_€/MWh": float(price) * 1.1  # Converting from $/MWh to €/MWh with approximation
            })
    
    df = pd.DataFrame(records)
    return df[["date", "price_€/MWh"]] if not df.empty else pd.DataFrame(columns=["date", "price_€/MWh"])

def fetch_nord_pool_prices(date: datetime = None) -> pd.DataFrame:
    """Fetch Nordic electricity market prices from Nord Pool."""
    if not NORD_POOL_API_KEY:
        raise ValueError("NORD_POOL_API_KEY not configured")
        
    date = date or datetime.utcnow()
    day = date.strftime("%d-%m-%Y")
    
    url = "https://www.nordpoolgroup.com/api/marketdata/page/10"
    headers = {
        "Authorization": f"Bearer {NORD_POOL_API_KEY}",
        "User-Agent": "Mozilla/5.0"
    }
    
    params = {
        "currency": "EUR",
        "endDate": day
    }
    
    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    
    data = resp.json()
    
    # Parse the Nord Pool data
    records = []
    try:
        rows = data.get('data', {}).get('Rows', [])
        for row in rows:
            if 'Name' in row and row.get('IsExtraRow', False) is False:
                hour = int(row['Name'].replace(':', '')) // 100
                
                for column in row.get('Columns', []):
                    if column.get('Name') == 'Oslo':  # Example area
                        price = float(column.get('Value', '0').replace(' ', '').replace(',', '.'))
                        records.append({
                            "date": pd.to_datetime(date.strftime("%Y-%m-%d")) + pd.to_timedelta(hour, unit="h"),
                            "price_€/MWh": price
                        })
    except Exception as e:
        print(f"Error parsing Nord Pool data: {e}")
    
    return pd.DataFrame(records)[["date", "price_€/MWh"]] if records else pd.DataFrame(columns=["date", "price_€/MWh"])

def save_prices(df: pd.DataFrame, path: str = "data/raw") -> None:
    """Appends today's prices to CSV (creates if missing)."""
    out = f"{path}/prices_{datetime.utcnow().strftime('%Y%m%d')}.csv"
    df.to_csv(out, index=False)

def analyze_price_volatility(df: pd.DataFrame, window_size: int = 24) -> pd.DataFrame:
    """
    Calculate rolling volatility metrics for price data.
    
    Args:
        df: DataFrame with 'date' and 'price_€/MWh' columns
        window_size: Window for rolling calculations (default: 24 hours)
    
    Returns:
        DataFrame with volatility metrics
    """
    volatility_df = df.copy().set_index('date')
    volatility_df['rolling_std'] = volatility_df['price_€/MWh'].rolling(window=window_size).std()
    volatility_df['rolling_range'] = volatility_df['price_€/MWh'].rolling(window=window_size).max() - \
                                     volatility_df['price_€/MWh'].rolling(window=window_size).min()
    volatility_df['volatility_ratio'] = volatility_df['rolling_std'] / volatility_df['price_€/MWh'].rolling(window=window_size).mean()
    return volatility_df.reset_index()

def detect_price_anomalies(df: pd.DataFrame, z_score_threshold: float = 2.5) -> pd.DataFrame:
    """
    Detect anomalous price points using z-score method.
    
    Args:
        df: DataFrame with 'date' and 'price_€/MWh' columns
        z_score_threshold: Threshold for anomaly detection
        
    Returns:
        DataFrame with anomaly flags
    """
    df_copy = df.copy()
    df_copy['price_mean'] = df_copy['price_€/MWh'].mean()
    df_copy['price_std'] = df_copy['price_€/MWh'].std()
    df_copy['z_score'] = (df_copy['price_€/MWh'] - df_copy['price_mean']) / df_copy['price_std']
    df_copy['is_anomaly'] = abs(df_copy['z_score']) > z_score_threshold
    return df_copy

def calculate_peak_off_peak_ratio(df: pd.DataFrame) -> float:
    """
    Calculate the ratio between peak (8am-8pm) and off-peak prices.
    
    Args:
        df: DataFrame with 'date' and 'price_€/MWh' columns
    
    Returns:
        Peak to off-peak price ratio
    """
    df_copy = df.copy()
    df_copy['hour'] = df_copy['date'].dt.hour
    peak_mask = (df_copy['hour'] >= 8) & (df_copy['hour'] < 20)
    peak_price = df_copy.loc[peak_mask, 'price_€/MWh'].mean()
    off_peak_price = df_copy.loc[~peak_mask, 'price_€/MWh'].mean()
    return peak_price / off_peak_price if off_peak_price != 0 else float('nan')

def fetch_historical_prices(start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    Fetch historical price data between two dates.
    
    Args:
        start_date: Start date for historical data
        end_date: End date for historical data
        
    Returns:
        DataFrame with historical price data
    """
    all_data = []
    current_date = start_date
    
    while current_date <= end_date:
        try:
            df = fetch_day_ahead_prices(date=current_date)
            all_data.append(df)
            print(f"Fetched data for {current_date.strftime('%Y-%m-%d')}")
        except Exception as e:
            print(f"Error fetching data for {current_date.strftime('%Y-%m-%d')}: {e}")
        
        current_date += pd.Timedelta(days=1)
    
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()

def analyze_seasonal_patterns(df: pd.DataFrame) -> dict:
    """
    Analyze seasonal patterns in electricity prices.
    
    Args:
        df: DataFrame with 'date' and 'price_€/MWh' columns
        
    Returns:
        Dictionary with seasonal pattern metrics
    """
    df_copy = df.copy()
    df_copy['hour'] = df_copy['date'].dt.hour
    df_copy['day'] = df_copy['date'].dt.day_name()
    df_copy['month'] = df_copy['date'].dt.month_name()
    df_copy['weekend'] = df_copy['date'].dt.dayofweek >= 5
    
    # Hourly patterns
    hourly_pattern = df_copy.groupby('hour')['price_€/MWh'].mean()
    
    # Day of week patterns
    daily_pattern = df_copy.groupby('day')['price_€/MWh'].mean()
    
    # Weekend vs weekday
    weekend_vs_weekday = {
        'weekend_avg': df_copy[df_copy['weekend']]['price_€/MWh'].mean(),
        'weekday_avg': df_copy[~df_copy['weekend']]['price_€/MWh'].mean(),
        'ratio': df_copy[df_copy['weekend']]['price_€/MWh'].mean() / 
                df_copy[~df_copy['weekend']]['price_€/MWh'].mean() 
                if df_copy[~df_copy['weekend']]['price_€/MWh'].mean() != 0 else float('nan')
    }
    
    return {
        'hourly_pattern': hourly_pattern,
        'daily_pattern': daily_pattern,
        'weekend_vs_weekday': weekend_vs_weekday
    }

def generate_mock_price_data(date=None):
    """Generate mock electricity price data for testing."""
    date = date or datetime.utcnow()
    day = date.strftime("%Y%m%d")
    
    mock_df = pd.DataFrame({
        'position': range(1, 25),
        'price.amount': [random.uniform(30, 70) for _ in range(24)]
    })
    
    mock_df["hour"] = mock_df["position"].astype(int) - 1
    mock_df["date"] = pd.to_datetime(day, format="%Y%m%d") \
                   + pd.to_timedelta(mock_df["hour"], unit="h")
    
    return mock_df.rename(columns={"price.amount": "price_€/MWh"})[["date", "price_€/MWh"]]

def get_day_ahead_prices(date: datetime = None) -> pd.DataFrame:
    """
    Smart wrapper that fetches data from multiple sources based on availability.
    Falls back to alternative sources if the primary source fails.
    """
    date = date or datetime.utcnow()
    
    # If mock data is explicitly requested
    if DATA_SOURCE.lower() == "mock":
        print("Using mock data (explicitly configured)")
        return generate_mock_price_data(date)
    
    # Try sources in order of preference
    sources = []
    
    # Add configured sources to the list
    if DATA_SOURCE.lower() == "auto" or DATA_SOURCE.lower() == "all":
        # Add all configured sources
        if ENTSOE_API_KEY:
            sources.append(("ENTSOE", fetch_day_ahead_prices))
        if ELEXON_API_KEY:
            sources.append(("ELEXON", fetch_elexon_prices))
        if EIA_API_KEY:
            sources.append(("EIA", fetch_eia_prices))
        if NORD_POOL_API_KEY:
            sources.append(("NORD_POOL", fetch_nord_pool_prices))
    elif DATA_SOURCE.lower() == "entsoe" and ENTSOE_API_KEY:
        sources.append(("ENTSOE", fetch_day_ahead_prices))
    elif DATA_SOURCE.lower() == "elexon" and ELEXON_API_KEY:
        sources.append(("ELEXON", fetch_elexon_prices))
    elif DATA_SOURCE.lower() == "eia" and EIA_API_KEY:
        sources.append(("EIA", fetch_eia_prices))
    elif DATA_SOURCE.lower() == "nordpool" and NORD_POOL_API_KEY:
        sources.append(("NORD_POOL", fetch_nord_pool_prices))
    
    # If no valid sources are configured, use mock data
    if not sources:
        print("No valid data sources configured, using mock data")
        return generate_mock_price_data(date)
    
    # Try each source in order
    errors = []
    for source_name, fetch_function in sources:
        try:
            print(f"Attempting to fetch data from {source_name}")
            df = fetch_function(date)
            if not df.empty:
                print(f"Successfully fetched data from {source_name}")
                return df
        except Exception as e:
            error_message = f"Error fetching from {source_name}: {e}"
            print(error_message)
            errors.append(error_message)
    
    # If all sources fail, use mock data
    print(f"All data sources failed:\n" + "\n".join(errors))
    print("Falling back to mock data")
    return generate_mock_price_data(date)