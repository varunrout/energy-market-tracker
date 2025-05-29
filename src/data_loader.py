import streamlit as st
from .fetching.price_fetchers import fetch_elexon_prices
from .transformation.data_transform import transform_data
from .feature_engineering.feature_builder import add_lag_features, add_rolling_features, add_calendar_features
import pandas as pd

@st.cache_data(show_spinner=False)
def load_price_data(date, dataset, bm_units):
    """
    Load and prepare price data: fetch, transform, and build features.
    """
    # Fetch raw data
    raw_df = fetch_elexon_prices(date=pd.to_datetime(date), dataset=dataset, bm_units=bm_units if dataset == "BOD" else None)
    if raw_df.empty:
        return raw_df
    # Normalize and fill gaps
    df = transform_data(raw_df)
    # Standardize timestamp column: rename any time/date column to 'date'
    time_col = next((c for c in df.columns if 'time' in c.lower() or 'date' in c.lower()), None)
    if time_col and time_col != 'date':
        df = df.rename(columns={time_col: 'date'})

    # Detect and standardize price column
    if 'price_€/MWh' not in df.columns:
        # 1) look for any column name containing 'price'
        price_candidates = [c for c in df.columns if 'price' in c.lower()]
        if price_candidates:
            df = df.rename(columns={price_candidates[0]: 'price_€/MWh'})
        else:
            # fallback: any numeric column
            num_cols = df.select_dtypes(include=['number']).columns.tolist()
            if num_cols:
                df = df.rename(columns={num_cols[0]: 'price_€/MWh'})

    # Feature engineering (only if price column standardized)
    if 'price_€/MWh' in df.columns:
        df = add_calendar_features(df, time_col='date')
        df = add_lag_features(df, ['price_€/MWh'])
        df = add_rolling_features(df, 'price_€/MWh')
    return df
