"""
Module for raw data transformations.
"""

import pandas as pd

def normalize_timestamp(df, time_col=None):
    """
    Ensure timestamp column is datetime and sorted.
    """
    if time_col is None:
        time_col = next((c for c in df.columns if "time" in c.lower() or "date" in c.lower()), None)
    if time_col:
        df[time_col] = pd.to_datetime(df[time_col])
        df = df.sort_values(time_col)
    return df


def fill_missing_gaps(df, time_col=None, freq='30min'):
    """
    Fill missing timestamps with NaNs for consistent time series.
    """
    if time_col is None:
        time_col = next((c for c in df.columns if "time" in c.lower() or "date" in c.lower()), None)
    if time_col:
        df = df.set_index(time_col)
        # Drop duplicate timestamps to allow reindexing
        df = df[~df.index.duplicated(keep='first')]
        df = df.asfreq(freq)
        df = df.reset_index()
    return df


def transform_data(df):
    """
    Apply all transformations: timestamp normalization and gap filling.
    """
    df = normalize_timestamp(df)
    df = fill_missing_gaps(df)
    return df
