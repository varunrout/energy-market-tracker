import streamlit as st
import pandas as pd
from src.analysis.price_analyzer import detect_price_anomalies

def render(df: pd.DataFrame, **kwargs):
    st.header("Anomaly Detection")
    if df is None or df.empty:
        st.warning("No data for anomaly detection.")
        return
    anomalies_df = detect_price_anomalies(df)
    if 'is_anomaly' not in anomalies_df.columns:
        st.warning("Anomaly detection not applicable.")
        return
    st.line_chart(anomalies_df.set_index('date')['price_€/MWh'])
    st.subheader("Anomalies Table")
    st.dataframe(anomalies_df[anomalies_df['is_anomaly']])
