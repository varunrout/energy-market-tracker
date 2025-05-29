import streamlit as st
import pandas as pd
from src.fetching.price_fetchers import fetch_elexon_average_system_prices
from datetime import datetime, timedelta

def render(df: pd.DataFrame, date, **kwargs):
    st.header("Generation Data (Average System Price)")
    # Use a default 7-day window for context
    to_date = date
    from_date = date - timedelta(days=7)
    from_str = from_date.strftime("%Y-%m-%d")
    to_str = to_date.strftime("%Y-%m-%d")
    data = fetch_elexon_average_system_prices(from_str, to_str)
    if data is None or 'data' not in data:
        st.warning("No generation data available.")
        return
    df_avg = pd.DataFrame(data['data'])
    if df_avg.empty:
        st.warning("Empty generation dataset.")
        return
    df_avg['settlementDate'] = pd.to_datetime(df_avg['settlementDate'])
    df_avg.set_index('settlementDate', inplace=True)
    price_cols = [c for c in df_avg.columns if 'price' in c.lower()]
    if price_cols:
        st.line_chart(df_avg[price_cols])
    st.subheader("Average System Price Data")
    st.dataframe(df_avg.reset_index())
