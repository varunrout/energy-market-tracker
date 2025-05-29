import os
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Any

from .. import config


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


def fetch_elexon_average_system_prices(from_date_str: str, to_date_str: str) -> Optional[Dict[str, Any]]:
    """
    Fetches average system prices from the ELEXON BMRS API (Insights Solution).
    Data is fetched only if ELEXON_API_KEY is available.
    """
    if not config.ELEXON_API_KEY:
        print("ELEXON_API_KEY not found. Skipping ELEXON average system prices fetch.")
        return None

    endpoint = f"{config.ELEXON_BASE_URL.rstrip('/')}" + f"{config.ELEXON_AVG_SYSTEM_PRICES_ENDPOINT_PATH}"
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

# Stubs for removed sources (ENTSO-E, EIA, Nord Pool, mock)
def fetch_day_ahead_prices(*args, **kwargs):
    print("ENTSO-E API is not supported in this deployment. Only ELEXON is available.")
    return pd.DataFrame()
def fetch_eia_prices(*args, **kwargs):
    print("EIA API is not supported in this deployment. Only ELEXON is available.")
    return pd.DataFrame()
def fetch_nord_pool_prices(*args, **kwargs):
    print("Nord Pool API is not supported in this deployment. Only ELEXON is available.")
    return pd.DataFrame()
def get_day_ahead_prices(*args, **kwargs):
    print("Only ELEXON data is available. Please use fetch_elexon_prices.")
    return pd.DataFrame(), "API issue: Only ELEXON supported"

