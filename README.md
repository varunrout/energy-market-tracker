# Energy Market Tracker

A comprehensive tool for tracking and analyzing electricity market prices with a focus on the UK market.

## Overview

This application fetches, processes, and visualizes day-ahead electricity prices from various energy markets. It provides insights into price trends, volatility, anomalies, and seasonal patterns.

## Features

- **Real-time Price Data**: Fetch current day-ahead electricity prices
- **Historical Analysis**: Retrieve and analyze historical price data
- **Multiple Data Sources**: Access data from various providers with automatic fallback
- **Interactive Visualizations**: Explore price trends through interactive charts
- **Advanced Analytics**: Detect anomalies, analyze volatility, and identify seasonal patterns

## Data Sources

The application can fetch data from multiple sources:

- **ENTSOE** (European Network of Transmission System Operators for Electricity): Primary source for UK and European electricity prices
- **Elexon** (British Electricity Trading and Transmission Arrangements): UK-specific electricity market data
- **EIA** (U.S. Energy Information Administration): US electricity market data
- **Nord Pool**: Nordic electricity market data
- **Mock Data**: Generated automatically when APIs are unavailable or for testing purposes

## Data Processing

### Data Collection
- Day-ahead hourly electricity prices are fetched for the selected date
- Prices are typically in €/MWh (Euros per Megawatt hour)
- Data includes timestamp and corresponding price for each hour

### Transformations & Analysis
- **Volatility Analysis**: Calculates rolling standard deviation, price range, and volatility ratio
- **Anomaly Detection**: Uses z-score method to identify unusual price points
- **Peak/Off-Peak Analysis**: Compares prices during peak hours (8am-8pm) vs off-peak hours
- **Seasonal Pattern Recognition**: Analyzes hourly, daily, and weekend vs weekday patterns

## Visualizations

The application provides several interactive charts and analytics:

1. **Price Overview**:
   - Time series chart of electricity prices
   - Price distribution histogram
   - Key metrics (average, min, max prices)
   - Daily averages table

2. **Volatility Analysis**:
   - Rolling standard deviation chart
   - Volatility ratio visualization
   - Price range analysis
   - Daily volatility summary

3. **Anomaly Detection**:
   - Interactive chart highlighting price anomalies
   - Configurable z-score threshold
   - Detailed anomaly information

4. **Seasonal Patterns**:
   - Hourly price trends throughout the day
   - Day of week price comparison
   - Weekend vs weekday price analysis

## Getting Started

### Prerequisites
- Python 3.7+
- Required Python packages listed in `requirements.txt`

### Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/energy-market-tracker.git
   cd energy-market-tracker
   ```

2. Set up environment variables in a `.env` file:
   ```
   ENTSOE_API_KEY=your_entsoe_api_key
   ELEXON_API_KEY=your_elexon_api_key
   EIA_API_KEY=your_eia_api_key
   NORD_POOL_API_KEY=your_nordpool_api_key
   DATA_SOURCE=elexon  # Options: auto, mock, entsoe, elexon, eia, nordpool
   ```

3. Run the start script:
   ```
   bash start.sh
   ```

4. To fetch fresh data:
   ```
   bash scripts/run_etl.sh
   ```

## Usage

After starting the application:

1. Navigate to the Streamlit interface (typically http://localhost:8501)
2. Use the sidebar to select different analysis views
3. Adjust date ranges to analyze specific periods
4. Use the "Refresh Today's Data" button to fetch the latest prices

## Project Structure

- `/src`: Core functionality modules
  - `data_fetch.py`: Data retrieval functions for various APIs
  - `config.py`: Configuration settings
- `/data/raw`: Stored price data
- `/notebooks`: Jupyter notebooks for data exploration
- `/scripts`: Utility scripts
- `/outputs/figures`: Generated charts and visualizations
- `app.py`: Main Streamlit application

## License

This project is licensed under the MIT License - see the LICENSE file for details.
