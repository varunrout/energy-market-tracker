# File: app.py

import streamlit as st
from src.categories.data_explorer import show as show_data_explorer
from src.categories.volatility_risk import show as show_volatility_risk

st.set_page_config(page_title="Electricity Dashboard", layout="wide")

menu = st.sidebar.radio(
    "Select Category",
    ("Data Explorer", "Forecasting & Backtest", "Volatility & Risk",
     "Regimes & Anomalies", "Profiles & Seasonal Patterns",
     "Causality & Policy Influence", "Simulation & Scenario Analysis")
)

if menu == "Data Explorer":
    show_data_explorer()
elif menu == "Volatility & Risk":
    show_volatility_risk()
else:
    st.info("Other categories will be implemented next.")
