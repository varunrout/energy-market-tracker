#!/usr/bin/env python
"""
Test script to verify the AGWS and ATL fixes with the latest API response formats.
"""

import pandas as pd
from datetime import datetime, timedelta
from src.fetching.elexon_client import ElexonApiClient

def test_atl():
    """Test the Actual Total Load endpoint."""
    print("\n===== Testing Actual Total Load (ATL) =====")
    client = ElexonApiClient()
    
    # Use a recent date range
    from_date = (datetime.utcnow() - timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
    to_date = (datetime.utcnow() - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    print(f"Date range: {from_date} to {to_date}")
    
    # Use the demand/actual/total endpoint
    df_atl = client.call_endpoint(
        "demand/actual/total",
        query_params={"from": from_date, "to": to_date}
    )
    
    print(f"ATL DataFrame shape: {df_atl.shape}")
    if not df_atl.empty:
        print(f"ATL columns: {df_atl.columns.tolist()}")
        print("First row:")
        print(df_atl.iloc[0])
        
        # Process timestamps
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
        
        print("\nProcessed dataframe:")
        print(df_atl[["ts", "quantity"]].head())
    else:
        print("WARNING: No ATL data returned")

def test_agws():
    """Test the Actual Wind & Solar Generation endpoint."""
    print("\n===== Testing Actual Wind & Solar Generation (AGWS) =====")
    client = ElexonApiClient()
    
    # Use a recent date range
    from_date = (datetime.utcnow() - timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
    to_date = (datetime.utcnow() - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    print(f"Date range: {from_date} to {to_date}")
    
    # Use the generation/actual/per-type/wind-and-solar endpoint
    df_agws = client.call_endpoint(
        "generation/actual/per-type/wind-and-solar",
        query_params={"from": from_date, "to": to_date}
    )
    
    print(f"AGWS DataFrame shape: {df_agws.shape}")
    if not df_agws.empty:
        print(f"AGWS columns: {df_agws.columns.tolist()}")
        print("First row:")
        print(df_agws.iloc[0])
        
        # Process timestamps
        if "startTime" in df_agws.columns:
            df_agws["ts"] = pd.to_datetime(df_agws["startTime"], utc=True)
        elif "local_datetime" in df_agws.columns:
            df_agws["ts"] = pd.to_datetime(df_agws["local_datetime"], utc=True)
        else:
            df_agws["settDate"] = pd.to_datetime(df_agws["settlementDate"])
            df_agws["ts"] = (
                df_agws["settDate"]
                + pd.to_timedelta((df_agws["settlementPeriod"] - 1) * 30, unit="m")
            )
            df_agws["ts"] = df_agws["ts"].dt.tz_localize("UTC")
        
        # Process the wind and solar data
        if "businessType" in df_agws.columns and "psrType" in df_agws.columns:
            print("\nProcessing by psrType...")
            # Create a pivot table
            df_agws_reset = df_agws.reset_index()
            pivot_agws = df_agws_reset.pivot_table(
                values="quantity", 
                index="ts", 
                columns="psrType", 
                aggfunc="sum"
            ).fillna(0)
            
            print("\nPivot table:")
            print(pivot_agws.head())
            
            # Create combined columns
            if "Wind Offshore" in pivot_agws.columns and "Wind Onshore" in pivot_agws.columns:
                pivot_agws["Wind Total"] = pivot_agws["Wind Offshore"] + pivot_agws["Wind Onshore"]
            
            if "Solar" in pivot_agws.columns:
                if "Wind Total" in pivot_agws.columns:
                    pivot_agws["Wind+Solar"] = pivot_agws["Wind Total"] + pivot_agws["Solar"]
                elif "Wind Offshore" in pivot_agws.columns and "Wind Onshore" in pivot_agws.columns:
                    pivot_agws["Wind+Solar"] = pivot_agws["Wind Offshore"] + pivot_agws["Wind Onshore"] + pivot_agws["Solar"]
            
            print("\nProcessed pivot table:")
            print(pivot_agws.head())
        else:
            print("\nSimple quantity processing:")
            print(df_agws[["ts", "quantity"]].head())
    else:
        print("WARNING: No AGWS data returned")

if __name__ == "__main__":
    test_atl()
    test_agws()
