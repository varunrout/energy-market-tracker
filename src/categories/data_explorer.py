# File: src/categories/data_explorer.py

import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, time
from src.fetching.elexon_client import ElexonApiClient


def show():
    """
    Data Explorer page that fetches only:
      1) APXMIDP (APX day-ahead index price + volume)
      2) Actual Total Load (ATL / B0610)
      3) Actual Wind & Solar Outturn (AGWS / B1630)
      4) Fuel-type Half-hourly Outturn (FUELHH)
    and renders each with Plotly visualizations.
    """

    st.title("Data Explorer: Actuals & APX Prices")

    client = ElexonApiClient()

    # ----------------------------------------------------------------------------
    # Sidebar: Global Date/Time Range
    # ----------------------------------------------------------------------------
    st.sidebar.markdown("### Select Date & Time Window")
    today = datetime.utcnow().date()
    default_start = today - pd.Timedelta(days=7)
    default_end = today

    start_date = st.sidebar.date_input("Start Date", value=default_start)
    start_time = st.sidebar.time_input("Start Time", value=time(0, 0))
    end_date = st.sidebar.date_input("End Date", value=default_end)
    end_time = st.sidebar.time_input("End Time", value=time(23, 30))

    dt_start = pd.to_datetime(f"{start_date} {start_time}").tz_localize("UTC")
    dt_end = pd.to_datetime(f"{end_date} {end_time}").tz_localize("UTC")
    if dt_start > dt_end:
        st.sidebar.error("Start Date/Time must be before End Date/Time.")
        st.stop()
        
    # Limit the date range to 14 days and don't request future data
    # Elexon API returns 400 Bad Request for large date ranges or future dates
    if dt_end > pd.Timestamp.now(tz="UTC"):
        st.sidebar.warning("End date is in the future. Using current time instead.")
        dt_end = pd.Timestamp.now(tz="UTC")
        
    if (dt_end - dt_start).days > 14:
        st.sidebar.warning("Date range exceeds 14 days. Limiting to 14 days.")
        dt_start = dt_end - pd.Timedelta(days=14)

    from_str = dt_start.strftime("%Y-%m-%dT%H:%M:%SZ")
    to_str = dt_end.strftime("%Y-%m-%dT%H:%M:%SZ")

    st.sidebar.markdown("---")
    st.sidebar.markdown(
        "This page shows:\n"
        "- APX Day-Ahead Price (APXMIDP)\n"
        "- Actual Total Load (ATL)\n"
        "- Actual Wind & Solar Outturn (AGWS)\n"
        "- Fuel-Type Generation Outturn (FUELHH)\n"
    )

    # ----------------------------------------------------------------------------
    # Tabs: Price & Demand | Wind | Fuel
    # ----------------------------------------------------------------------------
    tab1, tab2, tab3 = st.tabs(["Price & Demand", "Wind & Solar", "Fuel Outturn"])

    # ----------------------------------------------------------------------------
    # Tab 1: APXMIDP & ATL
    # ----------------------------------------------------------------------------
    with tab1:
        st.header("APX Day-Ahead Price & Actual Total Load")

        # 1) Fetch MID stream, filter APXMIDP
        df_mid = client.call_endpoint(
            "datasets/MID/stream",
            path_params={"dataset": "MID"},
            query_params={"from": from_str, "to": to_str}
        )
        if df_mid.empty:
            st.warning("MID endpoint returned no data for this window.")
        else:
            # Filter only APXMIDP rows
            if "dataProvider" in df_mid.columns:
                df_apx = df_mid[df_mid["dataProvider"] == "APXMIDP"].copy()
            else:
                df_apx = df_mid.copy()

            if df_apx.empty:
                st.warning("No APXMIDP rows found in this window.")
            else:
                # Build datetime index
                if "local_datetime" in df_apx.columns:
                    df_apx["ts"] = pd.to_datetime(df_apx["local_datetime"], utc=True)
                else:
                    df_apx["settDate"] = pd.to_datetime(df_apx["settlementDate"])
                    df_apx["ts"] = (
                        df_apx["settDate"]
                        + pd.to_timedelta((df_apx["settlementPeriod"] - 1) * 30, unit="m")
                    )
                    df_apx["ts"] = df_apx["ts"].dt.tz_localize("UTC")
                df_apx.set_index("ts", inplace=True)
                df_apx.sort_index(inplace=True)

                st.subheader("APX Day-Ahead Price (APXMIDP)")
                st.dataframe(
                    df_apx[["settlementDate", "settlementPeriod", "price", "volume"]]
                    .rename(columns={"price": "Price (p/MWh)", "volume": "Volume (MW)"})
                    .head(10)
                )

                # Plot: Price over time
                fig_price = px.line(
                    df_apx.reset_index(),
                    x="ts",
                    y="price",
                    labels={"ts": "Timestamp", "price": "APX Price (p/MWh)"},
                    title="APX Day-Ahead Price over Time"
                )
                st.plotly_chart(fig_price, use_container_width=True)

                # Plot: Volume over time (if nonzero)
                if df_apx["volume"].abs().sum() > 0:
                    fig_vol = px.bar(
                        df_apx.reset_index(),
                        x="ts",
                        y="volume",
                        labels={"ts": "Timestamp", "volume": "Volume (MW)"},
                        title="APX Auction Volume over Time"
                    )
                    st.plotly_chart(fig_vol, use_container_width=True)
                else:
                    st.info("APXMIDP 'volume' is zero for all timestamps.")

        st.markdown("---")

        # 2) Fetch Actual Total Load (ATL)
        st.subheader("Actual Total Load (ATL / B0610)")

        # Use the demand/actual/total endpoint which provides the actual total load data
        df_atl = client.call_endpoint(
            "demand/actual/total",
            query_params={"from": from_str, "to": to_str}
        )
        if df_atl.empty:
            st.warning("No Actual Total Load (ATL) data returned.")
        else:
            # Convert to datetime index
            if "startTime" in df_atl.columns:
                df_atl["ts"] = pd.to_datetime(df_atl["startTime"], utc=True)
            elif "local_datetime" in df_atl.columns:
                df_atl["ts"] = pd.to_datetime(df_atl["local_datetime"], utc=True)
            else:
                df_atl["settDate"] = pd.to_datetime(df_atl["settlementDate"])
                df_atl["ts"] = (
                    df_atl["settDate"]
                    + pd.to_timedelta((df_atl["settlementPeriod"] - 1) * 30, unit="m")
                )
                df_atl["ts"] = df_atl["ts"].dt.tz_localize("UTC")
            df_atl.set_index("ts", inplace=True)
            df_atl.sort_index(inplace=True)

            # Restrict to selected window
            df_atl = df_atl.loc[from_str:to_str]

            # Create a display dataframe with the relevant columns
            display_cols = ["settlementDate", "settlementPeriod", "quantity"]
            if all(col in df_atl.columns for col in display_cols):
                display_df = df_atl[display_cols].rename(columns={"quantity": "Load (MW)"})
            else:
                # If we don't have standard columns, show what we have
                display_df = df_atl.head(10)
                
            st.dataframe(display_df.head(10))
            
            # Plot the data
            if "quantity" in df_atl.columns:
                fig_load = px.line(
                    df_atl.reset_index(),
                    x="ts",
                    y="quantity",
                    labels={"ts": "Timestamp", "quantity": "Load (MW)"},
                    title="Actual Total Load (Half-Hourly)"
                )
                st.plotly_chart(fig_load, use_container_width=True)
            else:
                st.warning("Could not plot load data: 'quantity' column not found")
                st.write("Available columns:", df_atl.columns.tolist())

    # ----------------------------------------------------------------------------
    # Tab 2: Wind & Solar Outturn (AGWS)
    # ----------------------------------------------------------------------------
    with tab2:
        st.header("Actual Wind & Solar Generation (AGWS / B1630)")

        # Use the generation/actual/per-type/wind-and-solar endpoint instead of AGWS dataset
        df_agws = client.call_endpoint(
            "generation/actual/per-type/wind-and-solar",
            query_params={"from": from_str, "to": to_str}
        )
        if df_agws.empty:
            st.warning("No AGWS (wind+solar) data returned.")
        else:
            # Convert to datetime index
            if "startTime" in df_agws.columns:
                df_agws["ts"] = pd.to_datetime(df_agws["startTime"], utc=True)
                df_agws.set_index("ts", inplace=True)
            elif "local_datetime" in df_agws.columns:
                df_agws["ts"] = pd.to_datetime(df_agws["local_datetime"], utc=True)
                df_agws.set_index("ts", inplace=True)
            elif "settlementDate" in df_agws.columns and "settlementPeriod" in df_agws.columns:
                df_agws["settDate"] = pd.to_datetime(df_agws["settlementDate"])
                df_agws["ts"] = (
                    df_agws["settDate"]
                    + pd.to_timedelta((df_agws["settlementPeriod"] - 1) * 30, unit="m")
                )
                df_agws["ts"] = df_agws["ts"].dt.tz_localize("UTC")
                df_agws.set_index("ts", inplace=True)
            
            df_agws.sort_index(inplace=True)

            # Filter to the selected time window
            df_agws = df_agws.loc[from_str:to_str]

            # Process the wind and solar data based on businessType and psrType
            if "businessType" in df_agws.columns and "psrType" in df_agws.columns:
                # Create a pivot table to sum up quantities by timestamp and fuel type
                df_agws_reset = df_agws.reset_index()
                pivot_agws = df_agws_reset.pivot_table(
                    values="quantity", 
                    index="ts", 
                    columns="psrType", 
                    aggfunc="sum"
                ).fillna(0)
                
                # Create a combined Wind+Solar column
                if "Wind Offshore" in pivot_agws.columns and "Wind Onshore" in pivot_agws.columns:
                    pivot_agws["Wind Total"] = pivot_agws["Wind Offshore"] + pivot_agws["Wind Onshore"]
                
                if "Solar" in pivot_agws.columns:
                    if "Wind Total" in pivot_agws.columns:
                        pivot_agws["Wind+Solar"] = pivot_agws["Wind Total"] + pivot_agws["Solar"]
                    elif "Wind Offshore" in pivot_agws.columns and "Wind Onshore" in pivot_agws.columns:
                        pivot_agws["Wind+Solar"] = pivot_agws["Wind Offshore"] + pivot_agws["Wind Onshore"] + pivot_agws["Solar"]
                
                # Reset index for display
                display_df = pivot_agws.reset_index()
                
                # Create a simplified display dataframe
                show_cols = ["ts"]
                if "Wind+Solar" in pivot_agws.columns:
                    show_cols.append("Wind+Solar")
                elif "Wind Total" in pivot_agws.columns:
                    show_cols.append("Wind Total")
                
                if "Solar" in pivot_agws.columns:
                    show_cols.append("Solar")
                
                st.dataframe(display_df[show_cols].head(10))
                
                # Plot the data
                fig_agws = px.line(
                    display_df,
                    x="ts",
                    y=pivot_agws.columns.tolist(),
                    labels={"ts": "Timestamp", "value": "Generation (MW)", "variable": "Type"},
                    title="Actual Wind & Solar Generation (Half-Hourly)"
                )
                st.plotly_chart(fig_agws, use_container_width=True)
            else:
                # Create a display DataFrame with necessary columns
                display_cols = ["settlementDate", "settlementPeriod", "quantity"]
                if all(col in df_agws.columns for col in display_cols):
                    display_df = df_agws[display_cols].rename(columns={"quantity": "Wind+Solar (MW)"})
                else:
                    # If we don't have all required columns, just display what we have
                    display_df = df_agws.head(10)
                    
                st.dataframe(display_df.head(10))
                # Plot the data
                if "quantity" in df_agws.columns:
                    fig_agws = px.line(
                        df_agws.reset_index(),
                        x="ts",
                        y="quantity",
                        labels={"ts": "Timestamp", "quantity": "Wind+Solar (MW)"},
                        title="Actual Wind & Solar Generation (Half-Hourly)"
                    )
                    st.plotly_chart(fig_agws, use_container_width=True)
                else:
                    st.warning("Could not plot wind and solar data: 'quantity' column not found")
                    st.write("Available columns:", df_agws.columns.tolist())

    # ----------------------------------------------------------------------------
    # Tab 3: Fuel-Type Generation Outturn (FUELHH)
    # ----------------------------------------------------------------------------
    with tab3:
        st.header("Fuel-Type Generation Outturn (FUELHH / B1630)")

        # Fix: Use generation/actual/per-type endpoint instead of FUELHH/stream
        df_fuel = client.call_endpoint(
            "generation/actual/per-type",
            query_params={"from": from_str, "to": to_str}
        )
        if df_fuel.empty:
            st.warning("No FUELHH data returned.")
        else:
            # Process the nested 'data' column structure
            if 'data' in df_fuel.columns and isinstance(df_fuel.iloc[0]['data'], list):
                # Extract the list of dicts from the 'data' column
                expanded_data = []
                for idx, row in df_fuel.iterrows():
                    base_data = {'startTime': row['startTime'], 'settlementPeriod': row['settlementPeriod']}
                    for item in row['data']:
                        entry = base_data.copy()
                        entry.update(item)
                        expanded_data.append(entry)
                
                # Create a new DataFrame with the expanded data
                df_fuel = pd.DataFrame(expanded_data)
                
            # Convert to datetime index
            if "startTime" in df_fuel.columns:
                df_fuel["ts"] = pd.to_datetime(df_fuel["startTime"], utc=True)
                df_fuel.set_index("ts", inplace=True)
            elif "local_datetime" in df_fuel.columns:
                df_fuel["ts"] = pd.to_datetime(df_fuel["local_datetime"], utc=True)
                df_fuel.set_index("ts", inplace=True)
            elif "settlementDate" in df_fuel.columns and "settlementPeriod" in df_fuel.columns:
                df_fuel["settDate"] = pd.to_datetime(df_fuel["settlementDate"])
                df_fuel["ts"] = (
                    df_fuel["settDate"]
                    + pd.to_timedelta((df_fuel["settlementPeriod"] - 1) * 30, unit="m")
                )
                df_fuel["ts"] = df_fuel["ts"].dt.tz_localize("UTC")
                df_fuel.set_index("ts", inplace=True)
            
            df_fuel.sort_index(inplace=True)

            # Map field names to standardized columns
            # For expanded data from the per-type endpoint, we have psrType instead of fuelType
            if "fuelType" not in df_fuel.columns and "psrType" in df_fuel.columns:
                df_fuel = df_fuel.rename(columns={"psrType": "fuelType"})
                
            if "quantity" not in df_fuel.columns and "generation" in df_fuel.columns:
                # Rename the column to expected name
                df_fuel = df_fuel.rename(columns={"generation": "quantity"})
                
            if "fuelType" not in df_fuel.columns or "quantity" not in df_fuel.columns:
                st.error("FUELHH payload missing 'fuelType' or 'quantity' columns.")
                st.write("Available columns:", df_fuel.columns.tolist())
            else:
                st.markdown("**Preview (first 10 rows)**")
                st.dataframe(df_fuel[["fuelType", "quantity"]].head(10))

                # Pivot wide: index=ts, columns=fuelType, values=quantity
                pivot = df_fuel.pivot(columns="fuelType", values="quantity").fillna(0)
                # Reset index for Plotly
                pivot_reset = pivot.reset_index()

                st.markdown("**Line Chart by Fuel Type**")
                fig_fuel = px.line(
                    pivot_reset,
                    x="ts",
                    y=pivot.columns.tolist(),
                    labels={"ts": "Timestamp", "value": "Generation (MW)", "variable": "Fuel Type"},
                    title="Fuel-Type Generation (Half-Hourly)"
                )
                st.plotly_chart(fig_fuel, use_container_width=True)

