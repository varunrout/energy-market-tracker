import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any

# Import configurations from the central config module
from .. import config
from ..utils.mock_data_generator import generate_mock_price_data


def fetch_day_ahead_prices(date: datetime = None) -> pd.DataFrame:
    """Pulls 24 h of GB day-ahead prices for given date (UTC) from ENTSO-E."""
    if not config.ENTSOE_API_KEY:
        print("ENTSOE_API_KEY not configured. Skipping ENTSO-E fetch.")
        return pd.DataFrame(columns=["date", "price_€/MWh"])

    date = date or datetime.utcnow()
    day = date.strftime("%Y%m%d")
    params = {
        "documentType": config.ENTSOE_DOC_TYPE,
        "processType": config.ENTSOE_PROCESS_TYPE,
        "in_Domain": config.ENTSOE_IN_DOMAIN,
        "out_Domain": config.ENTSOE_OUT_DOMAIN,
        "periodStart": day + "0000",
        "periodEnd": day + "2359",
        "securityToken": config.ENTSOE_API_KEY
    }
    try:
        resp = requests.get(config.ENTSOE_API_URL, params=params)
        resp.raise_for_status()
        df = pd.read_xml(
            resp.content,
            xpath="//TimeSeries/Period/Point",
            namespaces={"": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:0"}
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


def fetch_elexon_prices(date: datetime = None) -> pd.DataFrame:
    """Fetch GB day-ahead prices from Elexon Insights API."""
    if not config.ELEXON_API_KEY:
        print("ELEXON_API_KEY not configured for Elexon day-ahead prices. Skipping.")
        return pd.DataFrame(columns=["date", "price_€/MWh"])
        
    date = date or datetime.utcnow()
    from_date_str = date.strftime("%Y-%m-%d")
    to_date_str = (date + timedelta(days=1)).strftime("%Y-%m-%d")
    
    url = f"{config.ELEXON_BASE_URL.rstrip('/')}{config.ELEXON_DAYAHEAD_AUCTION_ENDPOINT_PATH}"
    params = {
        "publishDateTimeFrom": f"{from_date_str}T00:00:00Z",
        "publishDateTimeTo": f"{to_date_str}T00:00:00Z",
        "format": "json",
    }
    headers = {"X-Api-Key": config.ELEXON_API_KEY} # Assuming X-Api-Key for this specific endpoint as per original

    try:
        resp = requests.get(url, params=params, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        
        records = []
        # The DayAheadAuction stream might return a list of records directly
        if isinstance(data, list):
            for item in data:
                # Adjust parsing based on actual Elexon API response structure for DayAheadAuction
                # This is a common structure, but verify with API docs
                settlement_date_str = item.get("settlementDate", from_date_str)
                settlement_period = item.get("settlementPeriod")
                price = item.get("price")

                if settlement_period is not None and price is not None:
                    hour = int(settlement_period) - 1 # Assuming settlementPeriod 1 is 00:00-00:30, 2 is 00:30-01:00 etc.
                                                    # For hourly data, this might need adjustment.
                                                    # If DayAheadAuction is hourly, settlementPeriod might map directly to hour.
                    
                    # Create datetime object for the specific hour
                    record_date = pd.to_datetime(settlement_date_str) + pd.to_timedelta(hour, unit="h") # Or half-hours if period is 30min
                    
                    records.append({
                        "date": record_date,
                        "price_€/MWh": float(price)
                    })
        
        if not records:
            return pd.DataFrame(columns=["date", "price_€/MWh"])
            
        df = pd.DataFrame(records)
        return df[["date", "price_€/MWh"]]
    except requests.exceptions.RequestException as e:
        print(f"Error fetching Elexon day-ahead data: {e}")
        return pd.DataFrame(columns=["date", "price_€/MWh"])
    except ValueError as e: # Handles JSON decoding errors or float conversion errors
        print(f"Error processing Elexon day-ahead data: {e}")
        return pd.DataFrame(columns=["date", "price_€/MWh"])


def fetch_eia_prices(date: datetime = None) -> pd.DataFrame:
    """Fetch US electricity prices from EIA API (uses regional pricing)."""
    if not config.EIA_API_KEY:
        print("EIA_API_KEY not configured. Skipping EIA.")
        return pd.DataFrame(columns=["date", "price_€/MWh"])
        
    date = date or datetime.utcnow()
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


def fetch_nord_pool_prices(date: datetime = None) -> pd.DataFrame:
    """Fetch Nordic electricity market prices from Nord Pool."""
    if not config.NORD_POOL_API_KEY: # Note: Nord Pool public API might not require a key, or might have other auth.
        print("NORD_POOL_API_KEY not configured (or not required by public endpoint). Skipping Nord Pool if key is essential.")
        # Depending on the actual Nord Pool API, you might proceed without a key or return empty.
        # For this example, we assume if a key variable exists, it's intended for use.
        # If the target API doesn't need a key, this check can be removed.
        return pd.DataFrame(columns=["date", "price_€/MWh"])
        
    date = date or datetime.utcnow()
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


def get_day_ahead_prices(date: datetime = None) -> pd.DataFrame:
    """
    Smart wrapper that fetches data from multiple sources based on availability.
    Falls back to alternative sources if the primary source fails.
    """
    date = date or datetime.utcnow()
    
    if config.DATA_SOURCE.lower() == "mock":
        print("Using mock data (explicitly configured)")
        return generate_mock_price_data(date)
    
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
                    print(f"API Key for {src_name} is not configured. Skipping.")
            else:
                print(f"Warning: Source '{src_name}' in PREFERRED_DATA_SOURCES is not a known source.")

    elif config.DATA_SOURCE.lower() in sources_config_map:
        src_name = config.DATA_SOURCE.lower()
        fetch_func, api_key_val = sources_config_map[src_name]
        if api_key_val or (src_name == "nordpool" and not api_key_val) : # Check if API key is configured or it's Nordpool without key
            active_sources_to_try.append((src_name.upper(), fetch_func))
        else:
            print(f"API Key for the specified source {src_name} is not configured. Cannot fetch.")
    
    if not active_sources_to_try:
        print("No valid data sources configured or specified source key missing, using mock data")
        return generate_mock_price_data(date)
    
    errors = []
    for source_name, fetch_function in active_sources_to_try:
        try:
            print(f"Attempting to fetch data from {source_name}")
            df = fetch_function(date)
            if not df.empty:
                print(f"Successfully fetched data from {source_name}")
                return df
            else:
                print(f"No data returned from {source_name} for the given date.")
        except Exception as e:
            error_message = f"Error fetching from {source_name}: {e}"
            print(error_message)
            errors.append(error_message)
    
    print(f"All attempted data sources failed or returned no data:\n" + "\n".join(errors))
    print("Falling back to mock data")
    return generate_mock_price_data(date)


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

