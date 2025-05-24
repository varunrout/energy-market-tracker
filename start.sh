#!/bin/bash

# Set script to exit on any error
set -e

echo "🔋 Starting Energy Market Tracker 🔋"
echo "--------------------------------"

# Check if virtual environment exists, create if not
if [ ! -d "venv" ]; then
  echo "Creating virtual environment..."
  python -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Update pip
echo "Updating pip..."
pip install --upgrade pip

# Install required dependencies
echo "Installing dependencies..."
if [ -f "requirements.txt" ]; then
  pip install -r requirements.txt
else
  echo "No requirements.txt found, installing common data science packages..."
  # Install common data science packages needed for the application
  pip install pandas matplotlib requests python-dotenv lxml plotly seaborn scikit-learn nbformat
fi

# Ensure visualization packages are installed
pip install matplotlib plotly --quiet

# Check if .env file exists and load it
if [ -f ".env" ]; then
  echo "Found .env configuration"
  export $(grep -v '^#' .env | xargs)
  echo "Using DATA_SOURCE=${DATA_SOURCE}"
else
  echo "Warning: No .env file found. Using default configuration."
fi

# Try to find the main application file
if [ -f "app.py" ]; then
  echo "Starting application from app.py..."
  python app.py
elif [ -f "main.py" ]; then
  echo "Starting application from main.py..."
  python main.py
elif [ -f "src/app.py" ]; then
  echo "Starting application from src/app.py..."
  python src/app.py
elif [ -f "src/main.py" ]; then
  echo "Starting application from src/main.py..."
  python src/main.py
else
  # If no main file found, run an example to fetch and display today's prices
  echo "No main application file found. Running example data fetch..."
  python -c "
import sys
sys.path.append('src')
from data_fetch import get_day_ahead_prices
import pandas as pd
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 1000)

print('Fetching today\\'s electricity prices...')
prices = get_day_ahead_prices()
print(prices)
print(f'Average price: {prices[\"price_€/MWh\"].mean():.2f} €/MWh')
print(f'Max price: {prices[\"price_€/MWh\"].max():.2f} €/MWh')
print(f'Min price: {prices[\"price_€/MWh\"].min():.2f} €/MWh')
"
fi

echo "--------------------------------"
echo "Application execution completed."
