import streamlit as st
import pandas as pd
import glob
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from src.data_fetch import (
    fetch_day_ahead_prices, save_prices, analyze_price_volatility, 
    detect_price_anomalies, calculate_peak_off_peak_ratio, 
    fetch_historical_prices, analyze_seasonal_patterns
)

st.set_page_config(
    page_title="UK Day-Ahead Electricity Price Analysis",
    layout="wide"
)

st.title("⚡️ UK Day-Ahead Electricity Market Insights")

# Sidebar controls
with st.sidebar:
    st.header("Controls")
    tab_options = ["Price Overview", "Volatility Analysis", "Anomaly Detection", "Seasonal Patterns"]
    selected_tab = st.radio("Select Analysis View", tab_options)
    
    if st.button("Refresh Today's Data"):
        df_new = fetch_day_ahead_prices()
        save_prices(df_new)
        st.success("Fetched new data!")
    
    start_date = st.date_input("Start date", pd.to_datetime("today") - pd.Timedelta(days=30))
    end_date = st.date_input("End date", pd.to_datetime("today"))
    
    st.subheader("Advanced Options")
    
    if st.button("Fetch Historical Data"):
        with st.spinner("Fetching historical data..."):
            historical_df = fetch_historical_prices(
                datetime.combine(start_date, datetime.min.time()), 
                datetime.combine(end_date, datetime.min.time())
            )
            if not historical_df.empty:
                save_path = f"data/raw/historical_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"
                historical_df.to_csv(save_path, index=False)
                st.success(f"Historical data saved to {save_path}")
            else:
                st.error("Failed to fetch historical data")

# Load all CSVs in data/raw
files = sorted(glob.glob("data/raw/prices_*.csv"))
historical_files = sorted(glob.glob("data/raw/historical_*.csv"))
files.extend(historical_files)

if not files:
    st.warning("No data found—run `scripts/run_etl.sh` or use 'Refresh Today's Data' first.")
else:
    df = pd.concat([pd.read_csv(f, parse_dates=["date"]) for f in files])
    df = df.sort_values('date').drop_duplicates()
    mask = (df["date"].dt.date >= start_date) & (df["date"].dt.date <= end_date)
    df_sel = df.loc[mask]
    
    if df_sel.empty:
        st.warning("No data available for selected date range.")
    else:
        if selected_tab == "Price Overview":
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Price Time Series")
                fig = px.line(df_sel, x="date", y="price_€/MWh", 
                              title="Day-Ahead Electricity Prices")
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.subheader("Price Distribution")
                fig = px.histogram(df_sel, x="price_€/MWh", nbins=30,
                                   title="Price Distribution")
                st.plotly_chart(fig, use_container_width=True)
            
            # Key metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Average Price", f"€{df_sel['price_€/MWh'].mean():.2f}")
            with col2:
                st.metric("Max Price", f"€{df_sel['price_€/MWh'].max():.2f}")
            with col3:
                st.metric("Min Price", f"€{df_sel['price_€/MWh'].min():.2f}")
            with col4:
                peak_off_peak = calculate_peak_off_peak_ratio(df_sel)
                st.metric("Peak/Off-Peak Ratio", f"{peak_off_peak:.2f}")
            
            # Daily averages table
            st.subheader("Daily Averages")
            daily = df_sel.copy()
            daily["date"] = daily["date"].dt.date
            daily = daily.groupby("date")["price_€/MWh"].agg(["mean", "min", "max", "std"]).reset_index()
            daily.columns = ["Date", "Mean (€/MWh)", "Min (€/MWh)", "Max (€/MWh)", "Std Dev"]
            daily = daily.round(2)
            st.dataframe(daily, use_container_width=True)
                
        elif selected_tab == "Volatility Analysis":
            st.subheader("Price Volatility Analysis")
            
            volatility_window = st.slider("Volatility Window (hours)", 4, 48, 24)
            
            volatility_df = analyze_price_volatility(df_sel, window_size=volatility_window)
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.line(volatility_df, x="date", y="rolling_std", 
                              title=f"Price Volatility (Rolling {volatility_window}h Std Dev)")
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                fig = px.line(volatility_df, x="date", y="volatility_ratio", 
                              title=f"Volatility Ratio (Std Dev/Mean)")
                st.plotly_chart(fig, use_container_width=True)
            
            st.subheader("Price Range Analysis")
            fig = px.line(volatility_df, x="date", y="rolling_range",
                          title=f"Price Range (Rolling {volatility_window}h Max-Min)")
            st.plotly_chart(fig, use_container_width=True)
            
            # Daily volatility table
            st.subheader("Daily Volatility Summary")
            daily_vol = volatility_df.copy()
            daily_vol["date"] = daily_vol["date"].dt.date
            daily_vol = daily_vol.groupby("date").agg({
                "price_€/MWh": ["mean", "std"],
                "rolling_std": "mean",
                "volatility_ratio": "mean"
            }).reset_index()
            daily_vol.columns = ["Date", "Mean Price", "Daily Std Dev", "Avg Rolling Std Dev", "Avg Volatility Ratio"]
            daily_vol = daily_vol.round(3)
            st.dataframe(daily_vol, use_container_width=True)
            
        elif selected_tab == "Anomaly Detection":
            st.subheader("Price Anomaly Detection")
            
            z_threshold = st.slider("Z-Score Threshold", 1.0, 4.0, 2.5, 0.1)
            anomaly_df = detect_price_anomalies(df_sel, z_score_threshold=z_threshold)
            
            # Plot with anomalies highlighted
            fig = go.Figure()
            
            # Add regular points
            fig.add_trace(go.Scatter(
                x=anomaly_df[~anomaly_df['is_anomaly']]['date'],
                y=anomaly_df[~anomaly_df['is_anomaly']]['price_€/MWh'],
                mode='lines+markers',
                name='Normal Price',
                line=dict(color='blue')
            ))
            
            # Add anomaly points
            fig.add_trace(go.Scatter(
                x=anomaly_df[anomaly_df['is_anomaly']]['date'],
                y=anomaly_df[anomaly_df['is_anomaly']]['price_€/MWh'],
                mode='markers',
                name='Anomaly',
                marker=dict(color='red', size=10, symbol='circle')
            ))
            
            fig.update_layout(title=f"Price Anomalies (Z-Score > {z_threshold})",
                             xaxis_title="Date", yaxis_title="Price (€/MWh)")
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Anomaly details
            if anomaly_df['is_anomaly'].any():
                st.subheader("Anomaly Details")
                anomaly_details = anomaly_df[anomaly_df['is_anomaly']].copy()
                anomaly_details = anomaly_details[['date', 'price_€/MWh', 'z_score']]
                anomaly_details.columns = ['Date', 'Price (€/MWh)', 'Z-Score']
                anomaly_details = anomaly_details.sort_values('Z-Score', ascending=False).reset_index(drop=True)
                st.dataframe(anomaly_details, use_container_width=True)
                
                st.info(f"Found {len(anomaly_details)} price anomalies using threshold z-score > {z_threshold}")
            else:
                st.success(f"No anomalies detected with threshold z-score > {z_threshold}")
                
        elif selected_tab == "Seasonal Patterns":
            st.subheader("Seasonal Price Patterns")
            
            patterns = analyze_seasonal_patterns(df_sel)
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Hourly patterns
                hourly = patterns['hourly_pattern'].reset_index()
                hourly.columns = ['Hour', 'Average Price']
                
                fig = px.line(hourly, x='Hour', y='Average Price',
                             title='Average Price by Hour of Day',
                             markers=True)
                fig.update_layout(xaxis_title='Hour of Day (0-23)', 
                                 yaxis_title='Average Price (€/MWh)')
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Daily patterns
                if 'daily_pattern' in patterns and not patterns['daily_pattern'].empty:
                    daily_pattern = patterns['daily_pattern'].reset_index()
                    daily_pattern.columns = ['Day', 'Average Price']
                    
                    # Sort days of week correctly
                    days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                    daily_pattern['Day'] = pd.Categorical(daily_pattern['Day'], categories=days_order, ordered=True)
                    daily_pattern = daily_pattern.sort_values('Day')
                    
                    fig = px.bar(daily_pattern, x='Day', y='Average Price',
                                title='Average Price by Day of Week')
                    fig.update_layout(xaxis_title='Day of Week', 
                                     yaxis_title='Average Price (€/MWh)')
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("Not enough data to analyze daily patterns")
            
            # Weekend vs Weekday comparison
            st.subheader("Weekend vs. Weekday Comparison")
            
            weekend_data = patterns['weekend_vs_weekday']
            weekend_df = pd.DataFrame({
                'Period': ['Weekday', 'Weekend'],
                'Average Price (€/MWh)': [weekend_data['weekday_avg'], weekend_data['weekend_avg']]
            })
            
            fig = px.bar(weekend_df, x='Period', y='Average Price (€/MWh)',
                        title='Weekend vs Weekday Price Comparison')
            st.plotly_chart(fig, use_container_width=True)
            
            st.metric("Weekend/Weekday Price Ratio", f"{weekend_data['ratio']:.2f}")
