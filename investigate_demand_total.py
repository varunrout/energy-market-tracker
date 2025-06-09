#!/usr/bin/env python
"""
Script to investigate the demand/actual/total endpoint for Actual Total Load data
"""

import requests
import json
import pandas as pd
from src import config

def inspect_endpoint(path, params=None):
    """Make a raw request to the Elexon API and inspect the response"""
    base_url = "https://data.elexon.co.uk/bmrs/api/v1"
    url = f"{base_url}{path}"
    headers = {"apiKey": config.ELEXON_API_KEY}
    
    print(f"\n==== Inspecting endpoint: {url} ====")
    print(f"With params: {params}")
    
    try:
        response = requests.get(url, headers=headers, params=params or {}, timeout=30)
        print(f"Status code: {response.status_code}")
        
        # Try to parse as JSON
        try:
            data = response.json()
            print(f"Response type: {type(data)}")
            
            # Pretty print the first part of the response
            if isinstance(data, dict):
                print("Keys in response:", list(data.keys()))
                if "data" in data:
                    data_value = data["data"]
                    print(f"Type of 'data' value: {type(data_value)}")
                    if isinstance(data_value, list):
                        print(f"Number of items in data list: {len(data_value)}")
                        if data_value:
                            print("First item in data list:")
                            print(json.dumps(data_value[0], indent=2)[:500] + "...")
                    elif isinstance(data_value, dict):
                        print("Data dict content:")
                        print(json.dumps(data_value, indent=2)[:500] + "...")
            elif isinstance(data, list):
                print(f"Number of items in response list: {len(data)}")
                if data:
                    print("First item in response list:")
                    print(json.dumps(data[0], indent=2)[:500] + "...")
                    
            return data
            
        except json.JSONDecodeError:
            print("Response is not valid JSON. Raw response (first 1000 chars):")
            print(response.text[:1000])
            
    except requests.RequestException as e:
        print(f"Error making request: {e}")
    
    return None

def main():
    # Test the demand/actual/total endpoint
    print("\n=== Testing demand/actual/total endpoint ===")
    
    # 1. Without parameters
    inspect_endpoint("/demand/actual/total")
    
    # 2. With date parameters in different formats
    inspect_endpoint("/demand/actual/total", {
        "from": "2023-01-01", 
        "to": "2023-01-02"
    })
    
    # 3. Try with settlementDate and settlementPeriod
    inspect_endpoint("/demand/actual/total", {
        "settlementDate": "2023-01-01"
    })
    
    # 4. Try with different date format
    inspect_endpoint("/demand/actual/total", {
        "from": "2023-01-01T00:00:00Z", 
        "to": "2023-01-02T23:59:59Z"
    })

if __name__ == "__main__":
    main()
