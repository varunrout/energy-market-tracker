import streamlit as st
import pandas as pd

def render(df: pd.DataFrame, **kwargs):
    """Render raw price data and time series."""
    st.header("Price Data")
    if df is None or df.empty:
        st.warning("No price data available for the selected parameters.")
        return
    # Attempt to plot price column
    price_cols = [c for c in df.columns if 'price' in c.lower()]
    if price_cols and 'date' in df.columns:
        df_plot = df.set_index('date')[price_cols]
        st.line_chart(df_plot)
    st.subheader("Data Table")
    st.dataframe(df.reset_index(drop=True))
