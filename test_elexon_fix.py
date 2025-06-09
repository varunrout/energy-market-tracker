#!/usr/bin/env python
"""
Test script to verify the fixed Elexon API client implementation with the three problematic endpoints:
1. Actual Total Load (ATL / B0610)
2. Actual Wind & Solar Generation (AGWS / B1630)
3. Fuel-Type Generation Outturn (FUELHH / B1630)
"""

import pandas as pd
from datetime import datetime, timedelta
from src.fetching.elexon_client import ElexonApiClient

def main():
    """Test the fixed Elexon API client implementation with problematic endpoints."""
    print("Testing fixed Elexon API client implementation...")
    
    # Initialize the client
    client = ElexonApiClient()
    
    # Set up the date range for testing - use fixed past dates for which we know data exists
    from_date = "2024-01-01T00:00:00Z"
    to_date = "2024-01-02T23:59:59Z"
    
    # Test 1: Actual Total Load (ATL) using demand/actual/total endpoint
    print("\n===== Test 1: Actual Total Load using demand/actual/total =====")
    df_atl = client.call_endpoint(
        "demand/actual/total",
        query_params={"from": from_date, "to": to_date}
    )
    print(f"ATL DataFrame shape: {df_atl.shape}")
    if not df_atl.empty:
        print(f"ATL columns: {df_atl.columns.tolist()}")
        print("First row:")
        print(df_atl.iloc[0])
        
        # Process timestamps for plotting
        if "startTime" in df_atl.columns:
            df_atl["ts"] = pd.to_datetime(df_atl["startTime"], utc=True)
            print("\nTimestamps successfully processed using 'startTime'")
        elif "local_datetime" in df_atl.columns:
            df_atl["ts"] = pd.to_datetime(df_atl["local_datetime"], utc=True)
            print("\nTimestamps successfully processed using 'local_datetime'")
        else:
            df_atl["settDate"] = pd.to_datetime(df_atl["settlementDate"])
            df_atl["ts"] = (
                df_atl["settDate"]
                + pd.to_timedelta((df_atl["settlementPeriod"] - 1) * 30, unit="m")
            )
            df_atl["ts"] = df_atl["ts"].dt.tz_localize("UTC")
            print("\nTimestamps successfully processed using 'settlementDate' and 'settlementPeriod'")
    else:
        print("WARNING: No ATL data returned")
    
    # Test 2: Actual Wind & Solar Generation (AGWS)
    print("\n===== Test 2: Actual Wind & Solar Generation =====")
    df_agws = client.call_endpoint(
        "generation/actual/per-type/wind-and-solar",
        query_params={"from": from_date, "to": to_date}
    )
    print(f"AGWS DataFrame shape: {df_agws.shape}")
    if not df_agws.empty:
        print(f"AGWS columns: {df_agws.columns.tolist()}")
        print("First row:")
        print(df_agws.iloc[0])
        
        # Process the different response structure
        if "businessType" in df_agws.columns and "psrType" in df_agws.columns:
            print("\nProcessing AGWS data with businessType and psrType...")
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
                
            # Create a pivot table
            df_agws_reset = df_agws.reset_index() if df_agws.index.name == 'ts' else df_agws
            pivot_agws = df_agws_reset.pivot_table(
                values="quantity", 
                index="ts", 
                columns="psrType", 
                aggfunc="sum"
            ).fillna(0)
            
            print("\nPivot table shape:", pivot_agws.shape)
            print("Pivot table columns:", pivot_agws.columns.tolist())
            
            # Create combined totals
            if "Wind Offshore" in pivot_agws.columns and "Wind Onshore" in pivot_agws.columns:
                pivot_agws["Wind Total"] = pivot_agws["Wind Offshore"] + pivot_agws["Wind Onshore"]
            
            if "Solar" in pivot_agws.columns:
                if "Wind Total" in pivot_agws.columns:
                    pivot_agws["Wind+Solar"] = pivot_agws["Wind Total"] + pivot_agws["Solar"]
                elif "Wind Offshore" in pivot_agws.columns and "Wind Onshore" in pivot_agws.columns:
                    pivot_agws["Wind+Solar"] = pivot_agws["Wind Offshore"] + pivot_agws["Wind Onshore"] + pivot_agws["Solar"]
            
            print("\nAGWS successfully processed with combined totals")
    else:
        print("WARNING: No AGWS data returned")
    
    # Test 3: Fuel-Type Generation Outturn (FUELHH)
    print("\n===== Test 3: Fuel-Type Generation Outturn =====")
    df_fuelhh = client.call_endpoint(
        "generation/actual/per-type",
        query_params={"from": from_date, "to": to_date}
    )
    print(f"FUELHH DataFrame shape: {df_fuelhh.shape}")
    if not df_fuelhh.empty:
        print(f"FUELHH columns: {df_fuelhh.columns.tolist()}")
        print("First row:")
        print(df_fuelhh.iloc[0])
        
        # Check if the response has a nested 'data' column that needs processing
        if 'data' in df_fuelhh.columns and isinstance(df_fuelhh.iloc[0]['data'], list):
            print("\nExpanding nested 'data' column...")
            # Extract the list of dicts from the 'data' column
            expanded_data = []
            for idx, row in df_fuelhh.iterrows():
                base_data = {'startTime': row['startTime'], 'settlementPeriod': row['settlementPeriod']}
                for item in row['data']:
                    entry = base_data.copy()
                    entry.update(item)
                    expanded_data.append(entry)
            
            expanded_df = pd.DataFrame(expanded_data)
            print(f"Expanded DataFrame shape: {expanded_df.shape}")
            print(f"Expanded columns: {expanded_df.columns.tolist()}")
            print("First few rows of expanded data:")
            print(expanded_df.head())
    else:
        print("WARNING: No FUELHH data returned")
    
    # Summary
    print("\n===== Summary =====")
    print(f"ATL: {'SUCCESS' if not df_atl.empty else 'FAILED'}")
    print(f"AGWS: {'SUCCESS' if not df_agws.empty else 'FAILED'}")
    print(f"FUELHH: {'SUCCESS' if not df_fuelhh.empty else 'FAILED'}")

if __name__ == "__main__":
    main()
