import pandas as pd
from datetime import datetime, timedelta
from typing import Optional

from ..fetching.price_fetchers import fetch_day_ahead_prices # Keep for fetch_historical_prices default
from .. import config


def analyze_price_volatility(df: pd.DataFrame, window_size: Optional[int] = None) -> pd.DataFrame:
    """
    Calculate rolling volatility metrics for price data.
    """
    current_window_size = window_size if window_size is not None else config.ANALYSIS_VOLATILITY_WINDOW_SIZE

    if 'price_€/MWh' not in df.columns or 'date' not in df.columns:
        print("DataFrame must contain 'date' and 'price_€/MWh' columns for volatility analysis.")
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame(columns=['date', 'rolling_std', 'rolling_range', 'volatility_ratio'])

    volatility_df = df.copy().set_index('date')
    volatility_df['rolling_std'] = volatility_df['price_€/MWh'].rolling(window=current_window_size, min_periods=1).std()
    volatility_df['rolling_range'] = volatility_df['price_€/MWh'].rolling(window=current_window_size, min_periods=1).max() - \
                                     volatility_df['price_€/MWh'].rolling(window=current_window_size, min_periods=1).min()
    rolling_mean = volatility_df['price_€/MWh'].rolling(window=current_window_size, min_periods=1).mean()
    volatility_df['volatility_ratio'] = volatility_df['rolling_std'] / rolling_mean
    volatility_df['volatility_ratio'].fillna(0, inplace=True) # Handle division by zero if mean is zero
    return volatility_df.reset_index()

def detect_price_anomalies(df: pd.DataFrame, z_score_threshold: Optional[float] = None) -> pd.DataFrame:
    """
    Detect anomalous price points using z-score method.
    """
    current_z_score_threshold = z_score_threshold if z_score_threshold is not None else config.ANALYSIS_ANOMALY_ZSCORE_THRESHOLD

    if 'price_€/MWh' not in df.columns:
        print("DataFrame must contain 'price_€/MWh' column for anomaly detection.")
        return pd.DataFrame()
    if df.empty or len(df) < 2: # Need at least 2 points for std dev
        df_copy = df.copy()
        df_copy['is_anomaly'] = False
        return df_copy

    df_copy = df.copy()
    price_mean = df_copy['price_€/MWh'].mean()
    price_std = df_copy['price_€/MWh'].std()
    
    if price_std == 0: # Avoid division by zero if all prices are the same
        df_copy['z_score'] = 0.0
    else:
        df_copy['z_score'] = (df_copy['price_€/MWh'] - price_mean) / price_std
    
    df_copy['is_anomaly'] = abs(df_copy['z_score']) > current_z_score_threshold
    return df_copy

def calculate_peak_off_peak_ratio(df: pd.DataFrame) -> float:
    """
    Calculate the ratio between peak (8am-8pm) and off-peak prices.
    """
    if 'price_€/MWh' not in df.columns or 'date' not in df.columns:
        print("DataFrame must contain 'date' and 'price_€/MWh' columns for peak/off-peak analysis.")
        return float('nan')
    if df.empty:
        return float('nan')

    df_copy = df.copy()
    df_copy['hour'] = df_copy['date'].dt.hour
    peak_mask = (df_copy['hour'] >= config.ANALYSIS_PEAK_HOUR_START) & \
                (df_copy['hour'] < config.ANALYSIS_PEAK_HOUR_END)
    
    peak_prices = df_copy.loc[peak_mask, 'price_€/MWh']
    off_peak_prices = df_copy.loc[~peak_mask, 'price_€/MWh']

    if peak_prices.empty or off_peak_prices.empty:
        print("Not enough data for both peak and off-peak periods.")
        return float('nan')

    peak_price_mean = peak_prices.mean()
    off_peak_price_mean = off_peak_prices.mean()
    
    return peak_price_mean / off_peak_price_mean if off_peak_price_mean != 0 else float('inf') # or nan

def fetch_historical_prices(start_date: datetime, end_date: datetime, 
                            fetch_function=fetch_day_ahead_prices) -> pd.DataFrame:
    """
    Fetch historical price data between two dates using the provided fetch_function.
    """
    all_data = []
    current_date = start_date
    
    while current_date <= end_date:
        try:
            print(f"Fetching historical data for {current_date.strftime('%Y-%m-%d')}")
            # Using the imported fetch_day_ahead_prices by default
            df = fetch_function(date=current_date) 
            if not df.empty:
                all_data.append(df)
            else:
                print(f"No data returned for {current_date.strftime('%Y-%m-%d')}")
        except Exception as e:
            print(f"Error fetching historical data for {current_date.strftime('%Y-%m-%d')}: {e}")
        
        current_date += timedelta(days=1)
    
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame(columns=["date", "price_€/MWh"]) # Ensure consistent empty DataFrame

def analyze_seasonal_patterns(df: pd.DataFrame) -> dict:
    """
    Analyze seasonal patterns in electricity prices.
    """
    if 'price_€/MWh' not in df.columns or 'date' not in df.columns:
        print("DataFrame must contain 'date' and 'price_€/MWh' columns for seasonal analysis.")
        return {}
    if df.empty:
        return {
            'hourly_pattern': pd.Series(dtype=float),
            'daily_pattern': pd.Series(dtype=float),
            'weekend_vs_weekday': {'weekend_avg': float('nan'), 'weekday_avg': float('nan'), 'ratio': float('nan')}
        }

    df_copy = df.copy()
    df_copy['hour'] = df_copy['date'].dt.hour
    df_copy['day_name'] = df_copy['date'].dt.day_name() # Use day_name for groupby
    df_copy['month_name'] = df_copy['date'].dt.month_name() # Use month_name
    df_copy['is_weekend'] = df_copy['date'].dt.dayofweek >= 5 # Saturday=5, Sunday=6
    
    hourly_pattern = df_copy.groupby('hour')['price_€/MWh'].mean()
    daily_pattern = df_copy.groupby('day_name')['price_€/MWh'].mean()
    
    weekend_prices = df_copy[df_copy['is_weekend']]['price_€/MWh']
    weekday_prices = df_copy[~df_copy['is_weekend']]['price_€/MWh']

    weekend_avg = weekend_prices.mean() if not weekend_prices.empty else float('nan')
    weekday_avg = weekday_prices.mean() if not weekday_prices.empty else float('nan')
    
    ratio = float('nan')
    if pd.notna(weekend_avg) and pd.notna(weekday_avg) and weekday_avg != 0:
        ratio = weekend_avg / weekday_avg
    
    weekend_vs_weekday = {
        'weekend_avg': weekend_avg,
        'weekday_avg': weekday_avg,
        'ratio': ratio
    }
    
    return {
        'hourly_pattern': hourly_pattern,
        'daily_pattern': daily_pattern,
        'weekend_vs_weekday': weekend_vs_weekday
    }
