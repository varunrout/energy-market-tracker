#!/usr/bin/env python
"""
Script to specifically investigate the B1610 endpoint for Actual Total Load data
"""

import requests
import json
import pandas as pd
from src import config

def inspect_b1610_endpoint(params):
    """Make a raw request to the B1610 endpoint and inspect the response"""
    base_url = "https://data.elexon.co.uk/bmrs/api/v1"
    path = "/datasets/B1610/stream"
    url = f"{base_url}{path}"
    headers = {"apiKey": config.ELEXON_API_KEY}
    
    print(f"\n==== Testing B1610 with params: {params} ====")
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        print(f"Status code: {response.status_code}")
        
        # Try to parse as JSON
        try:
            data = response.json()
            print(f"Response type: {type(data)}")
            
            # Pretty print the first part of the response
            if isinstance(data, list):
                print(f"Number of items in response list: {len(data)}")
                if data:
                    print("First item in response list:")
                    print(json.dumps(data[0], indent=2)[:500] + "...")
            elif isinstance(data, dict):
                print("Keys in response:", list(data.keys()))
                if "data" in data:
                    data_value = data["data"]
                    print(f"Type of 'data' value: {type(data_value)}")
                    if isinstance(data_value, list):
                        print(f"Number of items in data list: {len(data_value)}")
                        if data_value:
                            print("First item in data list:")
                            print(json.dumps(data_value[0], indent=2)[:500] + "...")
                    
            return data
            
        except json.JSONDecodeError:
            print("Response is not valid JSON. Raw response (first 1000 chars):")
            print(response.text[:1000])
            
    except requests.RequestException as e:
        print(f"Error making request: {e}")
    
    return None

def main():
    # Test different parameters for the B1610 endpoint
    
    # Try with various combinations of parameters
    # 1. Basic parameters
    inspect_b1610_endpoint({
        "from": "2023-01-01", 
        "to": "2023-01-02"
    })
    
    # 2. With bmUnit parameter
    inspect_b1610_endpoint({
        "from": "2023-01-01", 
        "to": "2023-01-02",
        "bmUnit": "T_CADL-1"
    })
    
    # 3. With documentType parameter
    inspect_b1610_endpoint({
        "from": "2023-01-01", 
        "to": "2023-01-02",
        "documentType": "INIT"
    })
    
    # 4. With both bmUnit and documentType
    inspect_b1610_endpoint({
        "from": "2023-01-01", 
        "to": "2023-01-02",
        "bmUnit": "T_CADL-1",
        "documentType": "INIT"
    })
    
    # 5. Try a different date range
    inspect_b1610_endpoint({
        "from": "2023-05-01", 
        "to": "2023-05-02",
        "bmUnit": "T_CADL-1"
    })
    
    # 6. Try the 'Actual Total Load' documentType
    inspect_b1610_endpoint({
        "from": "2023-01-01", 
        "to": "2023-01-02",
        "documentType": "Actual Total Load"
    })
    
    # 7. Try the 'documentType' with different capitalization
    inspect_b1610_endpoint({
        "from": "2023-01-01", 
        "to": "2023-01-02",
        "documenttype": "INIT"
    })

if __name__ == "__main__":
    main()
