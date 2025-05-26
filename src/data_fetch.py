import os
from datetime import datetime
import pandas as pd

# Load environment variables at the very beginning - NO LONGER NEEDED HERE
# import load_dotenv from dotenv - NO LONGER NEEDED HERE
# load_dotenv() - NO LONGER NEEDED HERE

# Imports from our new modules
# config module will load .env
from src import config # To access config.DEFAULT_SAVE_PATH if needed, or other direct configs
from src.fetching.price_fetchers import get_day_ahead_prices, fetch_elexon_average_system_prices
from src.analysis.price_analyzer import (
    analyze_price_volatility, 
    detect_price_anomalies,
    calculate_peak_off_peak_ratio,
)
from src.utils.file_operations import save_prices

if __name__ == "__main__":
    print("--- Running Energy Market Data Fetcher ---")

    # Create data/raw directory if it doesn't exist
    # save_prices will use config.DEFAULT_SAVE_PATH and create it
    # os.makedirs(config.DEFAULT_SAVE_PATH, exist_ok=True) # This is now handled by save_prices

    # Example 1: Fetch day-ahead prices using the smart wrapper
    print("\nFetching day-ahead prices (using get_day_ahead_prices smart wrapper)...")
    # Fetches for today by default, can pass specific date: get_day_ahead_prices(datetime(2023,1,1))
    today_prices_df, source_name = get_day_ahead_prices() 
    
    if not today_prices_df.empty:
        today_prices_df['source'] = source_name # Add source column
        print(f"\nSuccessfully fetched day-ahead prices from {source_name}:")
        print(today_prices_df.head())
        
        # Save the fetched prices (uses DEFAULT_SAVE_PATH from config if path not specified)
        save_prices(today_prices_df) 

        # Example: Analyze volatility
        # The analysis functions now use defaults from config if arguments are not passed
        volatility_data = analyze_price_volatility(today_prices_df)
        if not volatility_data.empty:
            print("\nPrice Volatility Analysis (first 5 rows):")
            print(volatility_data.head())

        # Example: Detect anomalies
        anomaly_data = detect_price_anomalies(today_prices_df)
        if not anomaly_data.empty:
            print("\nPrice Anomaly Detection (showing anomalies, if any):")
            print(anomaly_data[anomaly_data['is_anomaly']])

        # Example: Calculate peak/off-peak ratio
        ratio = calculate_peak_off_peak_ratio(today_prices_df)
        if pd.notna(ratio):
            print(f"\nPeak to Off-Peak Price Ratio: {ratio:.2f}")
        else:
            print("\nPeak to Off-Peak Price Ratio: Could not be calculated.")
    else:
        print(f"\nCould not fetch day-ahead prices using any source. Fallback source: {source_name}.")

    # Example 2: Fetch ELEXON average system prices (if API key is set)
    # This function directly uses os.getenv("ELEXON_API_KEY")
    print(f"\nAttempting to fetch ELEXON average system prices...")
    
    # Fetch data for a specific date range
    # Ensure ELEXON_API_KEY is set in your .env file for this to work
    from_date_str = "2024-01-01" # Example date
    to_date_str = "2024-01-01"   # Example date
    average_prices_data = fetch_elexon_average_system_prices(from_date_str=from_date_str, to_date_str=to_date_str)

    if average_prices_data:
        # The function already prints success/failure messages.
        # You can process average_prices_data (it's a dict)
        if 'data' in average_prices_data and average_prices_data['data']:
            print(f"Sample ELEXON average system price data point: {average_prices_data['data'][0]}")
        elif 'data' in average_prices_data and not average_prices_data['data']:
             print("ELEXON average system prices data received, but the data list is empty.")
        # else: (already handled by fetch_elexon_average_system_prices if 'data' key not found or other issues)
        #    print("ELEXON average system prices data received, but 'data' key not found or issue with data.")
    # else: (already handled by fetch_elexon_average_system_prices if API key missing or request failed)
    #    print("Failed to fetch ELEXON average system prices or API key not configured.")


    # Example of using fetch_historical_prices (from analysis module)
    # from src.analysis.price_analyzer import fetch_historical_prices # ensure imported
    # print("\nFetching historical prices for last 2 days...")
    # end_hist_date = datetime.utcnow() - timedelta(days=1)
    # start_hist_date = end_hist_date - timedelta(days=1) # Fetch 2 days of data
    # historical_df = fetch_historical_prices(start_hist_date, end_hist_date) # historical_df will now have 'source' column
    # if not historical_df.empty:
    #     print("\nFetched historical data (first 5 rows):")
    #     print(historical_df.head())
    #     save_prices(historical_df, path=os.path.join(config.DEFAULT_SAVE_PATH, "historical")) # Example of specifying sub-path
    # else:
    #     print("\nNo historical data fetched.")

    print("\n--- End of Energy Market Data Fetcher ---")