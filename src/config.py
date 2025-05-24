import os
from dotenv import load_dotenv

load_dotenv()

# Primary API keys
ENTSOE_API_KEY = os.getenv("ENTSOE_API_KEY")

# Data source configuration
DATA_SOURCE = os.getenv("DATA_SOURCE", "mock")  # Default to mock if not specified
