import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# API Keys & Data Source Configuration
ENTSOE_API_KEY = os.getenv("ENTSOE_API_KEY")
ELEXON_API_KEY = os.getenv("ELEXON_API_KEY")
EIA_API_KEY = os.getenv("EIA_API_KEY")
NORD_POOL_API_KEY = os.getenv("NORD_POOL_API_KEY")
DATA_SOURCE = os.getenv("DATA_SOURCE", "auto")

# API Endpoint Configurations
ELEXON_BASE_URL = os.getenv("ELEXON_BASE_URL", "https://data.elexon.co.uk/bmrs/api/v1")
ENTSOE_API_URL = os.getenv("ENTSOE_API_URL", "https://transparency.entsoe.eu/api")
EIA_API_URL = os.getenv("EIA_API_URL", "https://api.eia.gov/series/")
NORDPOOL_API_URL = os.getenv("NORDPOOL_API_URL", "https://www.nordpoolgroup.com/api/marketdata/page/10") # Example, may change

# ENTSO-E Specific Parameters
ENTSOE_DOC_TYPE = os.getenv("ENTSOE_DOC_TYPE", "A44")
ENTSOE_PROCESS_TYPE = os.getenv("ENTSOE_PROCESS_TYPE", "A01")
ENTSOE_IN_DOMAIN = os.getenv("ENTSOE_IN_DOMAIN", "10YGB----------A")
ENTSOE_OUT_DOMAIN = os.getenv("ENTSOE_OUT_DOMAIN", "10YGB----------A")

# ELEXON Specific Parameters
ELEXON_DAYAHEAD_AUCTION_ENDPOINT_PATH = os.getenv("ELEXON_DAYAHEAD_AUCTION_ENDPOINT_PATH", "/datasets/DayAheadAuction/stream")
ELEXON_AVG_SYSTEM_PRICES_ENDPOINT_PATH = os.getenv("ELEXON_AVG_SYSTEM_PRICES_ENDPOINT_PATH", "/balancing/system-prices/average")


# EIA Specific Parameters
EIA_SERIES_ID = os.getenv("EIA_SERIES_ID", "EBA.PJM-ALL.DF.H")
EIA_USD_TO_EUR_CONVERSION_RATE = float(os.getenv("EIA_USD_TO_EUR_CONVERSION_RATE", "0.92")) # Example rate

# Nord Pool Specific Parameters
NORDPOOL_CURRENCY = os.getenv("NORDPOOL_CURRENCY", "EUR")
NORDPOOL_AREA = os.getenv("NORDPOOL_AREA", "Oslo") # Example area

# Data Fetching Strategy
PREFERRED_DATA_SOURCES_STR = os.getenv("PREFERRED_DATA_SOURCES", "entsoe,elexon,eia,nordpool")
PREFERRED_DATA_SOURCES = [source.strip() for source in PREFERRED_DATA_SOURCES_STR.split(',')]

# Analysis Parameters
ANALYSIS_VOLATILITY_WINDOW_SIZE = int(os.getenv("ANALYSIS_VOLATILITY_WINDOW_SIZE", "24"))
ANALYSIS_ANOMALY_ZSCORE_THRESHOLD = float(os.getenv("ANALYSIS_ANOMALY_ZSCORE_THRESHOLD", "2.5"))
ANALYSIS_PEAK_HOUR_START = int(os.getenv("ANALYSIS_PEAK_HOUR_START", "8")) # 8 AM
ANALYSIS_PEAK_HOUR_END = int(os.getenv("ANALYSIS_PEAK_HOUR_END", "20"))   # Up to 8 PM (exclusive)

# Mock Data Parameters
MOCK_PRICE_MIN = float(os.getenv("MOCK_PRICE_MIN", "30.0"))
MOCK_PRICE_MAX = float(os.getenv("MOCK_PRICE_MAX", "70.0"))

# File Operations
DEFAULT_SAVE_PATH = os.getenv("DEFAULT_SAVE_PATH", "data/raw")

# Helper to get typed environment variables
def get_env_var(var_name, default_value, var_type=str):
    value = os.getenv(var_name, default_value)
    try:
        return var_type(value)
    except ValueError:
        print(f"Warning: Could not cast environment variable {var_name} to {var_type}. Using default: {default_value}")
        return default_value

# Example of re-defining a variable using the helper for robust type casting, if preferred:
# ANALYSIS_VOLATILITY_WINDOW_SIZE = get_env_var("ANALYSIS_VOLATILITY_WINDOW_SIZE", "24", int)

# Ensure critical API URLs end with a slash if they are base URLs for further path joining
if ELEXON_BASE_URL and not ELEXON_BASE_URL.endswith('/'):
    ELEXON_BASE_URL += '/'
if EIA_API_URL and not EIA_API_URL.endswith('/'):
    EIA_API_URL += '/'
# ENTSOE_API_URL and NORDPOOL_API_URL are typically full URLs, not base paths for joining.
