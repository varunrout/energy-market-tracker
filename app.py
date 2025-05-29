import streamlit as st
from datetime import datetime, timedelta
import pandas as pd
from src.data_loader import load_price_data
from src.pages import (
    price_data,
    volatility_analysis,
    anomaly_detection,
    peak_off_peak,
    seasonal_patterns,
    generation_data,
    forecasting
)

st.set_page_config(page_title="Energy Market Tracker", layout="wide")

# --- Sidebar controls ---
st.sidebar.title("Data Controls")
st.sidebar.markdown("This app visualizes electricity market datasets. Configure your query below.")

def_date = datetime.utcnow() - timedelta(days=1)
dataset = st.sidebar.selectbox(
    "Select ELEXON Dataset",
    ["MID", "BOAL", "BOD"],
    help="Choose which ELEXON dataset to visualize."
)
date = st.sidebar.date_input(
    "Select Date",
    def_date,
    help="Pick the date for which to fetch ELEXON data."
)
bm_units = []
if dataset == "BOD":
    bm_units_str = st.sidebar.text_input(
        "BM Units (comma-separated)",
        value="T_DRAXX-1,T_DIDCB-1",
        help="Enter one or more BM units, separated by commas. Required for BOD dataset."
    )
    bm_units = [u.strip() for u in bm_units_str.split(",") if u.strip()]

# --- Multipage navigation ---
pages = [
    "Price Data",
    "Volatility Analysis",
    "Anomaly Detection",
    "Peak/Off-Peak Ratio",
    "Seasonal Patterns",
    "Generation Data",
    "Forecasting"
]
page = st.sidebar.radio("Select Analysis Page", pages)

@st.cache_data(show_spinner=False)
def load_data(date, dataset, bm_units):
    return load_price_data(date=date, dataset=dataset, bm_units=bm_units)

df = load_data(date, dataset, bm_units)

# --- Page Routing ---
page_renderer = {
    "Price Data": price_data.render,
    "Volatility Analysis": volatility_analysis.render,
    "Anomaly Detection": anomaly_detection.render,
    "Peak/Off-Peak Ratio": peak_off_peak.render,
    "Seasonal Patterns": seasonal_patterns.render,
    "Generation Data": generation_data.render,
    "Forecasting": forecasting.render,
}

# Render the selected page
render_func = page_renderer.get(page)
if render_func:
    render_func(df=df, date=date, dataset=dataset, bm_units=bm_units)
else:
    st.error("Selected page not found.")

st.sidebar.markdown("---")
st.sidebar.info("Developed for electricity market data visualization.")
