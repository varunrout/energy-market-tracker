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

        df_atl = client.get_demand_actual_total()
        if df_atl.empty:
            st.warning("No Actual Total Load (ATL) data returned.")
        else:
            # Convert to datetime index
            if "local_datetime" in df_atl.columns:
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

            st.dataframe(
                df_atl[["settlementDate", "settlementPeriod", "quantity"]]
                .rename(columns={"quantity": "Load (MW)"})
                .head(10)
            )
            fig_load = px.line(
                df_atl.reset_index(),
                x="ts",
                y="quantity",
                labels={"ts": "Timestamp", "quantity": "Load (MW)"},
                title="Actual Total Load (Half-Hourly)"
            )
            st.plotly_chart(fig_load, use_container_width=True)

    # ----------------------------------------------------------------------------
    # Tab 2: Wind & Solar Outturn (AGWS)
    # ----------------------------------------------------------------------------
    with tab2:
        st.header("Actual Wind & Solar Generation (AGWS / B1630)")

        df_agws = client.call_endpoint(
            "datasets/AGWS/stream",
            path_params={"dataset": "AGWS"},
            query_params={"from": from_str, "to": to_str}
        )
        if df_agws.empty:
            st.warning("No AGWS (wind+solar) data returned.")
        else:
            # Convert to datetime index
            if "local_datetime" in df_agws.columns:
                df_agws["ts"] = pd.to_datetime(df_agws["local_datetime"], utc=True)
            else:
                df_agws["settDate"] = pd.to_datetime(df_agws["settlementDate"])
                df_agws["ts"] = (
                    df_agws["settDate"]
                    + pd.to_timedelta((df_agws["settlementPeriod"] - 1) * 30, unit="m")
                )
                df_agws["ts"] = df_agws["ts"].dt.tz_localize("UTC")
            df_agws.set_index("ts", inplace=True)
            df_agws.sort_index(inplace=True)

            df_agws = df_agws.loc[from_str:to_str]

            st.dataframe(
                df_agws[["settlementDate", "settlementPeriod", "quantity"]]
                .rename(columns={"quantity": "Wind+Solar (MW)"})
                .head(10)
            )
            fig_agws = px.line(
                df_agws.reset_index(),
                x="ts",
                y="quantity",
                labels={"ts": "Timestamp", "quantity": "Wind+Solar (MW)"},
                title="Actual Wind & Solar Generation (Half-Hourly)"
            )
            st.plotly_chart(fig_agws, use_container_width=True)

    # ----------------------------------------------------------------------------
    # Tab 3: Fuel-Type Generation Outturn (FUELHH)
    # ----------------------------------------------------------------------------
    with tab3:
        st.header("Fuel-Type Generation Outturn (FUELHH / B1630)")

        df_fuel = client.call_endpoint(
            "datasets/FUELHH/stream",
            path_params={"dataset": "FUELHH"},
            query_params={"from": from_str, "to": to_str}
        )
        if df_fuel.empty:
            st.warning("No FUELHH data returned.")
        else:
            # Convert to datetime index
            if "local_datetime" in df_fuel.columns:
                df_fuel["ts"] = pd.to_datetime(df_fuel["local_datetime"], utc=True)
            else:
                df_fuel["settDate"] = pd.to_datetime(df_fuel["settlementDate"])
                df_fuel["ts"] = (
                    df_fuel["settDate"]
                    + pd.to_timedelta((df_fuel["settlementPeriod"] - 1) * 30, unit="m")
                )
                df_fuel["ts"] = df_fuel["ts"].dt.tz_localize("UTC")
            df_fuel.set_index("ts", inplace=True)
            df_fuel.sort_index(inplace=True)

            df_fuel = df_fuel.loc[from_str:to_str]
            # We expect columns: fuelType, quantity
            if "fuelType" not in df_fuel.columns or "quantity" not in df_fuel.columns:
                st.error("FUELHH payload missing 'fuelType' or 'quantity' columns.")
            else:
                st.markdown("**Preview (first 10 rows)**")
                st.dataframe(df_fuel.head(10))

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

