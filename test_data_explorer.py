#!/usr/bin/env python
"""
Test script to validate the Data Explorer implementation with the Elexon API client fixes
"""

import pandas as pd
from datetime import datetime, timedelta
from src.categories.data_explorer import ElexonApiClient

def test_data_explorer_endpoints():
    """Test the endpoints used in data_explorer.py to verify they work correctly."""
    print("Testing Data Explorer endpoints...")
    
    # Initialize the client
    client = ElexonApiClient()
    
    # Set up the date range for testing
    today = datetime.now()
    from_date = (today - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00Z")
    to_date = today.strftime("%Y-%m-%dT23:59:59Z")
    
    # Test 1: Actual Total Load using demand/actual/total
    print("\n===== Test 1: Actual Total Load using demand/actual/total =====")
    df_atl = client.call_endpoint(
        "demand/actual/total",
        query_params={"from": from_date, "to": to_date}
    )
    print(f"ATL DataFrame shape: {df_atl.shape}")
    if not df_atl.empty:
        print(f"ATL columns: {df_atl.columns.tolist()}")
        print("Sample data:")
        print(df_atl.head(3))
    else:
        print("WARNING: No ATL data returned")
    
    # Test 2: Actual Wind & Solar Generation
    print("\n===== Test 2: Actual Wind & Solar Generation =====")
    df_agws = client.call_endpoint(
        "generation/actual/per-type/wind-and-solar",
        query_params={"from": from_date, "to": to_date}
    )
    print(f"AGWS DataFrame shape: {df_agws.shape}")
    if not df_agws.empty:
        print(f"AGWS columns: {df_agws.columns.tolist()}")
        print("Sample data:")
        print(df_agws.head(3))
    else:
        print("WARNING: No AGWS data returned")
    
    # Test 3: Fuel-Type Generation Outturn
    print("\n===== Test 3: Fuel-Type Generation Outturn =====")
    df_fuel = client.call_endpoint(
        "generation/actual/per-type",
        query_params={"from": from_date, "to": to_date}
    )
    print(f"FUELHH DataFrame shape: {df_fuel.shape}")
    if not df_fuel.empty:
        print(f"FUELHH columns: {df_fuel.columns.tolist()}")
        print("Sample data:")
        print(df_fuel.head(3))
        
        # Test the data expansion for the FUELHH data
        if 'data' in df_fuel.columns and isinstance(df_fuel.iloc[0]['data'], list):
            print("\nExpanding nested 'data' column...")
            # Extract the list of dicts from the 'data' column
            expanded_data = []
            for idx, row in df_fuel.iterrows():
                base_data = {'startTime': row['startTime'], 'settlementPeriod': row['settlementPeriod']}
                for item in row['data']:
                    entry = base_data.copy()
                    entry.update(item)
                    expanded_data.append(entry)
            
            expanded_df = pd.DataFrame(expanded_data)
            print(f"Expanded DataFrame shape: {expanded_df.shape}")
            print(f"Expanded columns: {expanded_df.columns.tolist()}")
            print("Sample of expanded data:")
            print(expanded_df.head(3))
    else:
        print("WARNING: No FUELHH data returned")
    
    # Summary
    print("\n===== Summary =====")
    print(f"ATL: {'SUCCESS' if not df_atl.empty else 'FAILED'}")
    print(f"AGWS: {'SUCCESS' if not df_agws.empty else 'FAILED'}")
    print(f"FUELHH: {'SUCCESS' if not df_fuel.empty else 'FAILED'}")
    print(f"FUELHH Expansion: {'SUCCESS' if not df_fuel.empty and 'data' in df_fuel.columns else 'FAILED or N/A'}")

if __name__ == "__main__":
    test_data_explorer_endpoints()
