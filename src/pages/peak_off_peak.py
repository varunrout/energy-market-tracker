import streamlit as st
import pandas as pd
from src.analysis.price_analyzer import calculate_peak_off_peak_ratio

def render(df: pd.DataFrame, **kwargs):
    st.header("Peak vs Off-Peak Ratio")
    if df is None or df.empty:
        st.warning("No data for peak/off-peak analysis.")
        return
    ratio = calculate_peak_off_peak_ratio(df)
    if pd.isna(ratio) or ratio == float('inf'):
        st.warning("Insufficient data to calculate peak/off-peak ratio.")
        return
    st.metric("Peak/Off-Peak Price Ratio", f"{ratio:.2f}")
