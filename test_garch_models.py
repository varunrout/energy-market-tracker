#!/usr/bin/env python3
"""
Test script to validate GARCH model implementation from the volatility analysis notebook
"""

import sys
sys.path.append('/workspaces/energy-market-tracker')

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from scipy import stats

# Import the Elexon client
from src.fetching.elexon_client import ElexonApiClient

# Test ARCH package availability
try:
    from arch import arch_model
    ARCH_AVAILABLE = True
    print("✓ ARCH package available for GARCH modeling")
except ImportError:
    ARCH_AVAILABLE = False
    print("✗ ARCH package not available")

def collect_data_safe(dataset, params, client=None):
    """Safely collect data from Elexon API using dataset streams."""
    if client is None:
        client = ElexonApiClient()
    
    try:
        data = client.get_dataset_stream(
            dataset=dataset,
            from_=params.get("from"),
            to=params.get("to")
        )
            
        if data is not None and len(data) > 0:
            print(f"   Successfully collected {len(data)} records")
            return data
        else:
            print(f"   No data returned for {dataset}")
            return pd.DataFrame()
    except Exception as e:
        print(f"   Error collecting data from {dataset}: {str(e)}")
        return pd.DataFrame()

def process_timestamps(df, time_col):
    """Process timestamp column and ensure proper datetime format."""
    if time_col not in df.columns:
        return df
    
    df[time_col] = pd.to_datetime(df[time_col], errors='coerce')
    df = df.dropna(subset=[time_col])
    df = df.sort_values(time_col)
    return df

def calculate_returns(prices, method='log'):
    """Calculate price returns."""
    if method == 'log':
        return np.log(prices / prices.shift(1)).dropna()
    elif method == 'simple':
        return (prices / prices.shift(1) - 1).dropna()
    else:
        raise ValueError("Method must be 'log' or 'simple'")

def test_garch_models():
    """Test the GARCH model implementation"""
    print("Testing GARCH Models Implementation")
    print("=" * 50)
    
    # Set up date range
    end_date = datetime(2024, 12, 31).date()
    start_date = end_date - timedelta(days=7)
    from_str = start_date.strftime("%Y-%m-%d")
    to_str = end_date.strftime("%Y-%m-%d")
    
    print(f"Collecting data from {from_str} to {to_str}")
    
    # Initialize client and collect data
    client = ElexonApiClient()
    df_mid = collect_data_safe("MID", {"from": from_str, "to": to_str}, client)
    
    if df_mid.empty:
        print("No data available for testing")
        return
    
    # Process price data
    df_price = df_mid[['startTime', 'settlementDate', 'settlementPeriod', 'price', 'volume', 'dataProvider']].copy()
    df_price = df_price.dropna(subset=['price'])
    
    # Filter for APXMIDP data
    if 'APXMIDP' in df_price['dataProvider'].values:
        df_price_main = df_price[df_price['dataProvider'] == 'APXMIDP'].copy()
        print(f"Using APXMIDP data: {df_price_main.shape[0]} records")
    else:
        df_price_main = df_price.copy()
    
    # Create price series
    df_price_main = df_price_main.sort_values('startTime').set_index('startTime')
    price_data = df_price_main['price'].replace([np.inf, -np.inf], np.nan).dropna()
    
    if len(price_data) < 50:
        print("Insufficient price data for GARCH modeling")
        return
    
    # Calculate returns
    log_returns = calculate_returns(price_data, method='log')
    print(f"Calculated {len(log_returns)} log returns")
    
    if not ARCH_AVAILABLE:
        print("ARCH package not available - skipping GARCH tests")
        return
    
    # Test GARCH models
    returns_clean = log_returns.dropna() * 100  # Convert to percentage
    
    print(f"\nTesting GARCH models with {len(returns_clean)} observations")
    print(f"Returns range: {returns_clean.min():.3f}% to {returns_clean.max():.3f}%")
    
    garch_models = {}
    
    # Test GARCH(1,1)
    try:
        print("\n1. Testing GARCH(1,1)...")
        garch_11 = arch_model(returns_clean, vol='Garch', p=1, q=1, rescale=False)
        garch_11_fit = garch_11.fit(disp='off')
        garch_models['GARCH(1,1)'] = garch_11_fit
        print(f"   ✓ GARCH(1,1) fitted successfully")
        print(f"   AIC: {garch_11_fit.aic:.3f}")
        print(f"   BIC: {garch_11_fit.bic:.3f}")
    except Exception as e:
        print(f"   ✗ GARCH(1,1) failed: {str(e)}")
    
    # Test EGARCH(1,1)
    try:
        print("\n2. Testing EGARCH(1,1)...")
        egarch_11 = arch_model(returns_clean, vol='EGARCH', p=1, o=1, q=1, rescale=False)
        egarch_11_fit = egarch_11.fit(disp='off')
        garch_models['EGARCH(1,1)'] = egarch_11_fit
        print(f"   ✓ EGARCH(1,1) fitted successfully")
        print(f"   AIC: {egarch_11_fit.aic:.3f}")
        print(f"   BIC: {egarch_11_fit.bic:.3f}")
    except Exception as e:
        print(f"   ✗ EGARCH(1,1) failed: {str(e)}")
    
    # Test GJR-GARCH(1,1)
    try:
        print("\n3. Testing GJR-GARCH(1,1)...")
        gjr_garch = arch_model(returns_clean, vol='GARCH', p=1, o=1, q=1, rescale=False)
        gjr_garch_fit = gjr_garch.fit(disp='off')
        garch_models['GJR-GARCH(1,1)'] = gjr_garch_fit
        print(f"   ✓ GJR-GARCH(1,1) fitted successfully")
        print(f"   AIC: {gjr_garch_fit.aic:.3f}")
        print(f"   BIC: {gjr_garch_fit.bic:.3f}")
    except Exception as e:
        print(f"   ✗ GJR-GARCH(1,1) failed: {str(e)}")
    
    if garch_models:
        print(f"\n✓ Successfully fitted {len(garch_models)} GARCH models")
        
        # Find best model
        comparison_df = pd.DataFrame({
            'Model': list(garch_models.keys()),
            'AIC': [model.aic for model in garch_models.values()],
            'BIC': [model.bic for model in garch_models.values()]
        })
        
        best_model_name = comparison_df.loc[comparison_df['AIC'].idxmin(), 'Model']
        print(f"Best model by AIC: {best_model_name}")
        
        # Test forecasting
        best_model = garch_models[best_model_name]
        try:
            forecasts = best_model.forecast(horizon=5)
            forecast_variance = forecasts.variance.iloc[-1].values
            forecast_volatility = np.sqrt(forecast_variance)
            print(f"\n✓ Volatility forecasting successful")
            print(f"   5-step ahead forecasts: {forecast_volatility}")
        except Exception as e:
            print(f"   ✗ Forecasting failed: {str(e)}")
    else:
        print("✗ No GARCH models successfully fitted")
    
    print("\nGARCH model testing completed!")

if __name__ == "__main__":
    test_garch_models()
