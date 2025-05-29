import streamlit as st
import pandas as pd
from src.analysis.price_analyzer import analyze_price_volatility

def render(df: pd.DataFrame, **kwargs):
    st.header("Volatility Analysis")
    if df is None or df.empty:
        st.warning("No data to analyze volatility.")
        return
    vol_df = analyze_price_volatility(df)
    if vol_df.empty:
        st.warning("Insufficient data for volatility analysis.")
        return
    st.line_chart(vol_df.set_index('date')[['rolling_std', 'rolling_range', 'volatility_ratio']])
    st.subheader("Volatility Data")
    st.dataframe(vol_df.reset_index(drop=True))
