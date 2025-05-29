import streamlit as st
import pandas as pd
from src.analysis.price_analyzer import analyze_seasonal_patterns

def render(df: pd.DataFrame, **kwargs):
    st.header("Seasonal Patterns")
    if df is None or df.empty:
        st.warning("No data for seasonal analysis.")
        return
    patterns = analyze_seasonal_patterns(df)
    # Hourly pattern
    st.subheader("Average Price by Hour")
    if not patterns['hourly_pattern'].empty:
        st.line_chart(patterns['hourly_pattern'])
    # Daily pattern
    st.subheader("Average Price by Day of Week")
    if not patterns['daily_pattern'].empty:
        st.bar_chart(patterns['daily_pattern'])
    # Weekend vs Weekday
    st.subheader("Weekend vs Weekday Average Price Ratio")
    ratio = patterns['weekend_vs_weekday']['ratio']
    if pd.notna(ratio):
        st.metric("Weekend/Weekday Ratio", f"{ratio:.2f}")
