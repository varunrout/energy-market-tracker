import os
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone # Added timezone
from typing import Optional, Dict, List, Any, Tuple

# Import configurations from the central config module
from .. import config
from ..utils.mock_data_generator import generate_mock_price_data


def fetch_day_ahead_prices(date: Optional[datetime] = None) -> pd.DataFrame:
    """Pulls 24 h of GB day-ahead prices for given date (UTC) from ENTSO-E."""
    if not config.ENTSOE_API_KEY:
        print("ENTSOE_API_KEY not configured. Skipping ENTSO-E fetch.")
        return pd.DataFrame(columns=["date", "price_€/MWh"])

    date = date or datetime.now(timezone.utc) # Changed to timezone.utc
    day = date.strftime("%Y%m%d")
    # In fetch_day_ahead_prices function:
    # Option 1: Current method (periodStart/periodEnd)
    params = {
        "documentType": config.ENTSOE_DOC_TYPE,
        "processType": config.ENTSOE_PROCESS_TYPE,
        "in_Domain": config.ENTSOE_IN_DOMAIN,
        "out_Domain": config.ENTSOE_OUT_DOMAIN,
        "periodStart": day + "0000",
        "periodEnd": day + "2359",
        "securityToken": config.ENTSOE_API_KEY
    }
    
    # Option 2: TimeInterval format (for POST requests)
    # params = {
    #     "documentType": config.ENTSOE_DOC_TYPE,
    #     "processType": config.ENTSOE_PROCESS_TYPE,
    #     "in_Domain": config.ENTSOE_IN_DOMAIN,
    #     "out_Domain": config.ENTSOE_OUT_DOMAIN,
    #     "TimeInterval": f"{date.strftime('%Y-%m-%d')}T00:00Z/{date.strftime('%Y-%m-%d')}T23:59Z",
    #     "securityToken": config.ENTSOE_API_KEY
    # }

    try:
        # For GET (current method):
        resp = requests.get(config.ENTSOE_API_URL, params=params)
        
        # For POST (alternate method):
        # resp = requests.post(config.ENTSOE_API_URL, data=params)
        print(f"ENTSO-E Request URL: {resp.url}") # Print the request URL
        resp.raise_for_status()
        # Add BytesIO import at the top of the file
        from io import BytesIO
        
        # Fix: Wrap resp.content in BytesIO to create a file-like object
        df = pd.read_xml(
            BytesIO(resp.content),
            xpath="//ns:TimeSeries/ns:Period/ns:Point",
            namespaces={"ns": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:0"}
        )
        df["hour"] = df["position"].astype(int) - 1
        df["date"] = pd.to_datetime(day, format="%Y%m%d") + pd.to_timedelta(df["hour"], unit="h")
        return df.rename(columns={"price.amount":"price_€/MWh"})[["date","price_€/MWh"]]
    except requests.exceptions.RequestException as e:
        print(f"Error fetching ENTSO-E data: {e}")
        return pd.DataFrame(columns=["date", "price_€/MWh"])
    except Exception as e: # Catch other parsing errors, e.g. if XML is malformed or empty
        print(f"Error processing ENTSO-E data: {e}")
        return pd.DataFrame(columns=["date", "price_€/MWh"])



def fetch_elexon_prices(date, dataset="MID", bm_units=None):
    """
    Fetch price data from Elexon for a specific dataset
    
    Parameters:
    -----------
    date : datetime
        The date for which to fetch data
    dataset : str
        The Elexon dataset code to fetch (MID, BOAL, BOD)
    bm_units : list
        List of BM Units to query (required for BOD dataset)
        
    Returns:
    --------
    DataFrame
        DataFrame containing price data
    """
    api_key = config.ELEXON_API_KEY
    if not api_key:
        print("Elexon API key not found in configuration")
        return pd.DataFrame()
    
    # Format date as RFC3339 full-date for settlement date
    settlement_date = date.strftime("%Y-%m-%d")
    
    results_df = pd.DataFrame()
    
    try:
        if dataset == "MID":
            # Market Index Data endpoint
            from_date = date.strftime("%Y-%m-%dT00:00:00Z")
            to_date = date.strftime("%Y-%m-%dT23:59:59Z")
            url = f"https://data.elexon.co.uk/bmrs/api/v1/datasets/MID?from={from_date}&to={to_date}&format=json"
            print(f"Requesting URL: {url}")
            
            response = requests.get(url, headers={"apiKey": api_key}, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            if "data" in data and data["data"]:
                results_df = pd.DataFrame(data["data"])
                
        elif dataset == "BOAL":
            # Bid-Offer Acceptance Levels endpoint - requires querying each settlement period
            dfs = []
            
            # Loop through all settlement periods (1-50 for a full day)
            for period in range(1, 51):
                # Construct URL with settlement date and period
                url = f"https://data.elexon.co.uk/bmrs/api/v1/balancing/acceptances/all?settlementDate={settlement_date}&settlementPeriod={period}&format=json"
                
                # Add BM Units if specified
                if bm_units:
                    bm_unit_params = "&".join([f"bmUnit={unit}" for unit in bm_units])
                    url = f"{url}&{bm_unit_params}"
                
                print(f"Requesting URL for period {period}: {url}")
                
                response = requests.get(url, headers={"apiKey": api_key}, timeout=30)
                
                # Skip if we get a 404 or other error for this period
                if response.status_code != 200:
                    print(f"No data for period {period} (Status: {response.status_code})")
                    continue
                
                data = response.json()
                if "data" in data and data["data"]:
                    period_df = pd.DataFrame(data["data"])
                    dfs.append(period_df)
            
            if dfs:
                results_df = pd.concat(dfs, ignore_index=True)
                
        elif dataset == "BOD":
            # Bid & Offer Data endpoint - requires BM units
            if not bm_units:
                print("BOD dataset requires BM units")
                return pd.DataFrame()
                
            dfs = []
            from_date = date.strftime("%Y-%m-%dT00:00:00Z")
            to_date = date.strftime("%Y-%m-%dT23:59:59Z")
            
            for bm_unit in bm_units:
                url = f"https://data.elexon.co.uk/bmrs/api/v1/balancing/bid-offer?bmUnit={bm_unit}&from={from_date}&to={to_date}&format=json"
                print(f"Requesting URL for BM Unit {bm_unit}: {url}")
                
                response = requests.get(url, headers={"apiKey": api_key}, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                if "data" in data and data["data"]:
                    unit_df = pd.DataFrame(data["data"])
                    unit_df["bmUnit"] = bm_unit  # Add the BM unit for reference
                    dfs.append(unit_df)
            
            if dfs:
                results_df = pd.concat(dfs, ignore_index=True)
        
        if not results_df.empty:
            # Add dataset type for reference
            results_df["dataset"] = dataset
            
            # Convert timestamps if available
            time_cols = [col for col in results_df.columns if 'time' in col.lower() or 'date' in col.lower()]
            for col in time_cols:
                try:
                    results_df[col] = pd.to_datetime(results_df[col])
                except:
                    pass  # Skip if conversion fails
                    
        return results_df
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {dataset} data from Elexon: {e}")
        return pd.DataFrame()


def fetch_eia_prices(date: Optional[datetime] = None) -> pd.DataFrame:
    """Fetch US electricity prices from EIA API (uses regional pricing)."""
    if not config.EIA_API_KEY:
        print("EIA_API_KEY not configured. Skipping EIA.")
        return pd.DataFrame(columns=["date", "price_€/MWh"])
        
    date = date or datetime.now(timezone.utc) # Changed to timezone.utc
    start_date_str = date.strftime("%Y%m%dT00")
    end_date_str = date.strftime("%Y%m%dT23")
    
    url = config.EIA_API_URL
    params = {
        "api_key": config.EIA_API_KEY,
        "series_id": config.EIA_SERIES_ID,
        "start": start_date_str,
        "end": end_date_str,
    }
    
    try:
        resp = requests.get(url, params=params)
        print(f"EIA Request URL: {resp.url}") # Print the request URL
        resp.raise_for_status()
        data = resp.json()
        
        records = []
        if 'series' in data and data['series'] and data['series'][0].get('data'):
            series_data = data['series'][0]['data']
            for timestamp_str, price_str in series_data:
                # EIA timestamps are often YYYYMMDDTHHZ
                dt = pd.to_datetime(timestamp_str, format='%Y%m%dT%HZ')
                price = float(price_str)
                records.append({
                    "date": dt,
                    "price_€/MWh": price * config.EIA_USD_TO_EUR_CONVERSION_RATE
                })
        
        df = pd.DataFrame(records)
        return df[["date", "price_€/MWh"]] if not df.empty else pd.DataFrame(columns=["date", "price_€/MWh"])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching EIA data: {e}")
        return pd.DataFrame(columns=["date", "price_€/MWh"])
    except (ValueError, KeyError) as e: # Handles JSON decoding, float conversion, or key errors
        print(f"Error processing EIA data: {e}")
        return pd.DataFrame(columns=["date", "price_€/MWh"])


def fetch_nord_pool_prices(date: Optional[datetime] = None) -> pd.DataFrame:
    """Fetch Nordic electricity market prices from Nord Pool."""
    if not config.NORD_POOL_API_KEY: # Note: Nord Pool public API might not require a key, or might have other auth.
        print("NORD_POOL_API_KEY not configured (or not required by public endpoint). Attempting fetch if endpoint is public, otherwise skipping.")
        # Depending on the actual Nord Pool API, you might proceed without a key or return empty.
        # For this example, we assume if a key variable exists, it's intended for use.
        # If the target API doesn't need a key, this check can be removed or modified.
        # The current logic will proceed to attempt the request if NORDPOOL_API_URL is set.
        if not config.NORDPOOL_API_URL: # If no URL, definitely skip
             return pd.DataFrame(columns=["date", "price_€/MWh"])
        
    date = date or datetime.now(timezone.utc) # Changed to timezone.utc
    day_str = date.strftime("%d-%m-%Y")
    
    url = config.NORDPOOL_API_URL
    headers = {
        # "Authorization": f"Bearer {config.NORD_POOL_API_KEY}", # Uncomment if Bearer token is used
        "User-Agent": "Mozilla/5.0"
    }
    params = {
        "currency": config.NORDPOOL_CURRENCY,
        "endDate": day_str,
    }
    
    try:
        resp = requests.get(url, headers=headers, params=params)
        print(f"Nord Pool Request URL: {resp.url}") # Print the request URL
        resp.raise_for_status()
        data = resp.json()
        
        records = []
        if 'data' in data and data['data'].get('Rows'):
            rows = data['data']['Rows']
            for row in rows:
                if not row.get('IsExtraRow', False) and 'Columns' in row and 'Name' in row:
                    # 'Name' usually contains the hour, e.g., "00 - 01" or "00:00"
                    hour_str = row['Name'].split(" - ")[0].split(":")[0] # Extract hour
                    hour = int(hour_str)
                    
                    for column in row['Columns']:
                        if column.get('Name') == config.NORDPOOL_AREA:
                            price_str = column.get('Value', '0').replace(',', '.').replace(' ', '')
                            price = float(price_str)
                            
                            record_date = pd.to_datetime(date.strftime("%Y-%m-%d")) + pd.to_timedelta(hour, unit="h")
                            records.append({
                                "date": record_date,
                                "price_€/MWh": price
                            })
                            break # Found price for the area for this hour
            
        df = pd.DataFrame(records)
        return df[["date", "price_€/MWh"]] if not df.empty else pd.DataFrame(columns=["date", "price_€/MWh"])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching Nord Pool data: {e}")
        return pd.DataFrame(columns=["date", "price_€/MWh"])
    except (ValueError, KeyError, TypeError) as e:
        print(f"Error processing Nord Pool data: {e}")
        return pd.DataFrame(columns=["date", "price_€/MWh"])


def get_day_ahead_prices(date: Optional[datetime] = None) -> Tuple[pd.DataFrame, str]:
    """
    Smart wrapper that fetches data from multiple sources based on availability.
    Falls back to alternative sources if the primary source fails.
    Returns a tuple containing the DataFrame and the name of the source.
    """
    date = date or datetime.now(timezone.utc) # Changed to timezone.utc
    
    if config.DATA_SOURCE.lower() == "mock":
        print("Using mock data (explicitly configured)")
        return generate_mock_price_data(date), "mock"
    
    sources_config_map = {
        "entsoe": (fetch_day_ahead_prices, config.ENTSOE_API_KEY),
        "elexon": (fetch_elexon_prices, config.ELEXON_API_KEY),
        "eia": (fetch_eia_prices, config.EIA_API_KEY),
        "nordpool": (fetch_nord_pool_prices, config.NORD_POOL_API_KEY) # Key might be optional for some public Nord Pool endpoints
    }
    
    active_sources_to_try = []
    
    if config.DATA_SOURCE.lower() in ["auto", "all"]:
        for src_name in config.PREFERRED_DATA_SOURCES:
            if src_name in sources_config_map:
                fetch_func, api_key_val = sources_config_map[src_name]
                # For sources like ENTSO-E, ELEXON, EIA, key is mandatory.
                # For NordPool, it might be optional depending on the endpoint.
                # This logic assumes key is required if present in config.
                if api_key_val: # Check if API key is configured (not None or empty)
                    active_sources_to_try.append((src_name.upper(), fetch_func))
                elif src_name == "nordpool" and not api_key_val: # Special case if Nordpool can work without key
                    print(f"Nordpool API key not set, but attempting as it might be a public endpoint.")
                    active_sources_to_try.append((src_name.upper(), fetch_func))
                else:
                    print(f"API Key for {src_name.upper()} is not configured. Skipping {src_name.upper()} in '{config.DATA_SOURCE}' mode.")
            else:
                print(f"Warning: Source '{src_name}' in PREFERRED_DATA_SOURCES is not a known source.")

    elif config.DATA_SOURCE.lower() in sources_config_map:
        src_name = config.DATA_SOURCE.lower()
        fetch_func, api_key_val = sources_config_map[src_name]
        if api_key_val or (src_name == "nordpool" and not api_key_val) : # Check if api key is configured or it's Nordpool without key
            active_sources_to_try.append((src_name.upper(), fetch_func))
        else:
            print(f"API Key for the specified source {src_name.upper()} is not configured. Cannot fetch from {src_name.upper()}.")
    
    if not active_sources_to_try:
        print("No valid data sources to attempt based on configuration (e.g., missing API key for specified source or all preferred sources). Using mock data.")
        return generate_mock_price_data(date), "mock"
    
    errors = []
    for source_name, fetch_function in active_sources_to_try:
        try:
            print(f"Attempting to fetch data from {source_name}")
            df = fetch_function(date)
            if not df.empty:
                print(f"Successfully fetched data from {source_name}")
                return df, source_name
            else:
                message = f"No data returned from {source_name} for the given date."
                print(message)
                errors.append(message)
        except Exception as e:
            error_message = f"Error fetching from {source_name}: {e}"
            print(error_message)
            errors.append(error_message)
    
    print(f"All attempted data sources in the current strategy failed or returned no data.")
    if errors:
        print("Errors encountered:")
        for err in errors:
            print(f"- {err}")
    print("Falling back to mock data.")
    return generate_mock_price_data(date), "mock"


def fetch_elexon_average_system_prices(from_date_str: str, to_date_str: str) -> Optional[Dict[str, Any]]:
    """
    Fetches average system prices from the ELEXON BMRS API (Insights Solution).
    Data is fetched only if ELEXON_API_KEY is available.
    """
    if not config.ELEXON_API_KEY:
        print("ELEXON_API_KEY not found. Skipping ELEXON average system prices fetch.")
        return None

    endpoint = f"{config.ELEXON_BASE_URL.rstrip('/')}{config.ELEXON_AVG_SYSTEM_PRICES_ENDPOINT_PATH}"
    params = {
        "fromSettlementDate": from_date_str,
        "toSettlementDate": to_date_str,
        "settlementPeriod": "*", 
        "format": "json",
        "scriptKey": config.ELEXON_API_KEY 
    }
    
    try:
        response = requests.get(endpoint, params=params)
        response.raise_for_status()
        print("Successfully fetched ELEXON average system prices.")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching ELEXON average system prices: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response status: {e.response.status_code}")
            print(f"Response text: {e.response.text}")
        return None
    except ValueError as e: # JSON decoding error
        print(f"Error decoding ELEXON average system prices JSON response: {e}")
        return None

