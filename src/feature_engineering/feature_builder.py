"""
Module for feature engineering on price series.
"""

import pandas as pd

def add_lag_features(df, cols, lags=[1,24]):
    """
    Add lag features for specified columns.
    """
    for col in cols:
        for lag in lags:
            df[f"{col}_lag{lag}"] = df[col].shift(lag)
    return df


def add_rolling_features(df, col, windows=[24,48]):
    """
    Add rolling mean and std features.
    """
    for w in windows:
        df[f"{col}_roll_mean_{w}"] = df[col].rolling(window=w, min_periods=1).mean()
        df[f"{col}_roll_std_{w}"] = df[col].rolling(window=w, min_periods=1).std()
    return df


def add_calendar_features(df, time_col='date'):
    """
    Add hour, weekday, month indicators.
    """
    df['hour'] = df[time_col].dt.hour
    df['weekday'] = df[time_col].dt.weekday
    df['month'] = df[time_col].dt.month
    df['is_weekend'] = df['weekday'] >= 5
    return df
