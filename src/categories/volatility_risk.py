# File: src/categories/volatility_risk.py

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime, time
import warnings
warnings.filterwarnings('ignore')

# Statistical libraries
from scipy import stats
from statsmodels.tsa.stattools import adfuller
from statsmodels.stats.diagnostic import het_arch

# Risk modeling
try:
    from arch import arch_model
    ARCH_AVAILABLE = True
except ImportError:
    ARCH_AVAILABLE = False

from src.fetching.elexon_client import ElexonApiClient


def safe_api_call(client, dataset, start_time, end_time, max_retries=3):
    """Safely call the API with error handling"""
    for attempt in range(max_retries):
        try:
            data = client.get_actual_total_load(start_time, end_time) if dataset == 'ATL' else \
                   client.get_mid_price_data(start_time, end_time) if dataset == 'MID' else \
                   client.get_fuel_outturn_summary(start_time, end_time)
            return data
        except Exception as e:
            if attempt == max_retries - 1:
                st.error(f"Failed to fetch {dataset} data after {max_retries} attempts: {str(e)}")
                return None
            st.warning(f"Attempt {attempt + 1} failed for {dataset}, retrying...")
    return None


def calculate_returns(prices, return_type='log'):
    """Calculate returns from price series"""
    if return_type == 'log':
        returns = np.log(prices / prices.shift(1))
    else:
        returns = prices.pct_change()
    return returns.dropna()


def calculate_volatility_metrics(returns):
    """Calculate comprehensive volatility and risk metrics"""
    metrics = {}
    
    # Basic statistics
    metrics['mean_return'] = returns.mean()
    metrics['volatility'] = returns.std()
    metrics['annualized_vol'] = returns.std() * np.sqrt(365.25 * 48)  # Half-hourly data
    metrics['skewness'] = stats.skew(returns.dropna())
    metrics['kurtosis'] = stats.kurtosis(returns.dropna())
    
    # Risk metrics
    metrics['var_95'] = np.percentile(returns.dropna(), 5)
    metrics['var_99'] = np.percentile(returns.dropna(), 1)
    metrics['cvar_95'] = returns[returns <= metrics['var_95']].mean()
    metrics['cvar_99'] = returns[returns <= metrics['var_99']].mean()
    
    # Maximum drawdown
    cumulative_returns = (1 + returns).cumprod()
    running_max = cumulative_returns.expanding().max()
    drawdown = (cumulative_returns - running_max) / running_max
    metrics['max_drawdown'] = drawdown.min()
    
    return metrics


def perform_statistical_tests(returns):
    """Perform statistical tests on returns"""
    results = {}
    
    # Stationarity test (ADF)
    adf_result = adfuller(returns.dropna())
    results['adf_statistic'] = adf_result[0]
    results['adf_pvalue'] = adf_result[1]
    results['is_stationary'] = adf_result[1] < 0.05
    
    # ARCH effects test
    try:
        arch_result = het_arch(returns.dropna(), nlags=5)
        results['arch_statistic'] = arch_result[0]
        results['arch_pvalue'] = arch_result[1]
        results['has_arch_effects'] = arch_result[1] < 0.05
    except:
        results['arch_statistic'] = None
        results['arch_pvalue'] = None
        results['has_arch_effects'] = None
    
    # Normality test
    jb_stat, jb_pvalue = stats.jarque_bera(returns.dropna())
    results['jb_statistic'] = jb_stat
    results['jb_pvalue'] = jb_pvalue
    results['is_normal'] = jb_pvalue > 0.05
    
    return results


def fit_garch_models(returns):
    """Fit GARCH family models"""
    if not ARCH_AVAILABLE:
        return None
    
    models = {}
    
    try:
        # GARCH(1,1)
        garch = arch_model(returns.dropna() * 100, vol='Garch', p=1, q=1)
        garch_fit = garch.fit(disp='off')
        models['GARCH'] = garch_fit
        
        # EGARCH(1,1)
        egarch = arch_model(returns.dropna() * 100, vol='EGARCH', p=1, q=1)
        egarch_fit = egarch.fit(disp='off')
        models['EGARCH'] = egarch_fit
        
        # GJR-GARCH(1,1)
        gjr = arch_model(returns.dropna() * 100, vol='GARCH', p=1, o=1, q=1)
        gjr_fit = gjr.fit(disp='off')
        models['GJR-GARCH'] = gjr_fit
        
    except Exception as e:
        st.error(f"Error fitting GARCH models: {str(e)}")
        return None
    
    return models


def create_volatility_dashboard(prices, returns, metrics, tests):
    """Create comprehensive volatility visualization dashboard"""
    fig = make_subplots(
        rows=3, cols=2,
        subplot_titles=[
            'Price Time Series',
            'Returns Distribution',
            'Returns Time Series',
            'Rolling Volatility (30-period)',
            'Q-Q Plot vs Normal',
            'Autocorrelation of Squared Returns'
        ],
        specs=[[{"secondary_y": False}, {"secondary_y": False}],
               [{"secondary_y": False}, {"secondary_y": False}],
               [{"secondary_y": False}, {"secondary_y": False}]]
    )
    
    # Price time series
    fig.add_trace(
        go.Scatter(x=prices.index, y=prices.values, name='Price', line=dict(color='blue')),
        row=1, col=1
    )
    
    # Returns distribution
    fig.add_trace(
        go.Histogram(x=returns.dropna(), name='Returns', nbinsx=50, opacity=0.7),
        row=1, col=2
    )
    
    # Returns time series
    fig.add_trace(
        go.Scatter(x=returns.index, y=returns.values, name='Returns', line=dict(color='red')),
        row=2, col=1
    )
    
    # Rolling volatility
    rolling_vol = returns.rolling(window=30).std() * np.sqrt(30)
    fig.add_trace(
        go.Scatter(x=rolling_vol.index, y=rolling_vol.values, name='Rolling Vol', line=dict(color='green')),
        row=2, col=2
    )
    
    # Q-Q plot
    theoretical_quantiles = stats.norm.ppf(np.linspace(0.01, 0.99, len(returns.dropna())))
    sample_quantiles = np.sort(returns.dropna())
    fig.add_trace(
        go.Scatter(x=theoretical_quantiles, y=sample_quantiles, mode='markers', name='Q-Q Plot'),
        row=3, col=1
    )
    # Add diagonal line
    min_val, max_val = min(theoretical_quantiles.min(), sample_quantiles.min()), max(theoretical_quantiles.max(), sample_quantiles.max())
    fig.add_trace(
        go.Scatter(x=[min_val, max_val], y=[min_val, max_val], mode='lines', 
                  name='Perfect Normal', line=dict(dash='dash', color='red')),
        row=3, col=1
    )
    
    # Autocorrelation of squared returns
    squared_returns = returns ** 2
    acf_values = [squared_returns.autocorr(lag=i) for i in range(1, 21)]
    fig.add_trace(
        go.Bar(x=list(range(1, 21)), y=acf_values, name='ACF Squared Returns'),
        row=3, col=2
    )
    
    fig.update_layout(height=900, showlegend=False, title_text="Volatility Analysis Dashboard")
    return fig


def create_risk_metrics_chart(metrics):
    """Create risk metrics visualization"""
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=['Risk Metrics', 'Distribution Properties', 'VaR Levels', 'Volatility Comparison'],
        specs=[[{"type": "indicator"}, {"type": "bar"}],
               [{"type": "bar"}, {"type": "indicator"}]]
    )
    
    # Risk metrics indicators
    fig.add_trace(
        go.Indicator(
            mode="gauge+number+delta",
            value=abs(metrics['max_drawdown']) * 100,
            title={'text': "Max Drawdown (%)"},
            gauge={'axis': {'range': [None, 100]},
                   'bar': {'color': "red"},
                   'steps': [{'range': [0, 20], 'color': "lightgreen"},
                            {'range': [20, 50], 'color': "yellow"},
                            {'range': [50, 100], 'color': "lightcoral"}],
                   'threshold': {'line': {'color': "red", 'width': 4},
                                'thickness': 0.75, 'value': 90}}
        ),
        row=1, col=1
    )
    
    # Distribution properties
    fig.add_trace(
        go.Bar(x=['Skewness', 'Excess Kurtosis'], 
               y=[metrics['skewness'], metrics['kurtosis']],
               name='Distribution'),
        row=1, col=2
    )
    
    # VaR levels
    fig.add_trace(
        go.Bar(x=['VaR 95%', 'VaR 99%', 'CVaR 95%', 'CVaR 99%'],
               y=[abs(metrics['var_95']) * 100, abs(metrics['var_99']) * 100,
                  abs(metrics['cvar_95']) * 100, abs(metrics['cvar_99']) * 100],
               name='Risk Measures'),
        row=2, col=1
    )
    
    # Volatility indicator
    fig.add_trace(
        go.Indicator(
            mode="gauge+number",
            value=metrics['annualized_vol'] * 100,
            title={'text': "Annualized Volatility (%)"},
            gauge={'axis': {'range': [None, 2000]},
                   'bar': {'color': "blue"},
                   'steps': [{'range': [0, 500], 'color': "lightgreen"},
                            {'range': [500, 1000], 'color': "yellow"},
                            {'range': [1000, 2000], 'color': "lightcoral"}]}
        ),
        row=2, col=2
    )
    
    fig.update_layout(height=600, title_text="Risk Metrics Dashboard")
    return fig


def monte_carlo_var(returns, garch_forecast=None, n_simulations=10000, confidence_level=0.05):
    """Calculate Monte Carlo VaR"""
    if garch_forecast is not None:
        # Use GARCH forecast for volatility
        simulated_returns = np.random.normal(
            returns.mean(), garch_forecast, n_simulations
        )
    else:
        # Use historical volatility
        simulated_returns = np.random.normal(
            returns.mean(), returns.std(), n_simulations
        )
    
    var_mc = np.percentile(simulated_returns, confidence_level * 100)
    return var_mc


def show():
    """Main Streamlit app for Volatility & Risk Analysis"""
    st.title("⚡ Volatility & Risk Analysis")
    st.markdown("""
    Comprehensive volatility modeling and risk analysis for UK electricity markets.
    This module provides advanced risk metrics, GARCH modeling, and stress testing capabilities.
    """)
    
    # Sidebar configuration
    st.sidebar.markdown("### Analysis Configuration")
    
    # Date range selection
    today = datetime.utcnow().date()
    default_start = today - pd.Timedelta(days=14)
    default_end = today
    
    start_date = st.sidebar.date_input("Start Date", value=default_start)
    end_date = st.sidebar.date_input("End Date", value=default_end)
    
    # Convert to datetime
    dt_start = pd.to_datetime(start_date).tz_localize("UTC")
    dt_end = pd.to_datetime(end_date).tz_localize("UTC")
    
    if dt_start > dt_end:
        st.sidebar.error("Start date must be before end date.")
        st.stop()
    
    if (dt_end - dt_start).days > 14:
        st.sidebar.warning("Date range limited to 14 days for API performance.")
        dt_start = dt_end - pd.Timedelta(days=14)
    
    # Analysis options
    return_type = st.sidebar.selectbox("Return Type", ["log", "simple"])
    confidence_levels = st.sidebar.multiselect(
        "VaR Confidence Levels", 
        [90, 95, 99], 
        default=[95, 99]
    )
    
    # Data loading
    client = ElexonApiClient()
    
    with st.spinner("Fetching market data..."):
        # Try MID data first (most reliable)
        price_data = safe_api_call(client, 'MID', dt_start, dt_end)
        
        if price_data is None or len(price_data) == 0:
            st.error("Unable to fetch price data. Please try a different date range.")
            st.stop()
    
    # Process the data
    try:
        df = pd.DataFrame(price_data)
        if 'settlementDate' in df.columns and 'settlementPeriod' in df.columns:
            # Convert settlement date and period to datetime
            df['datetime'] = pd.to_datetime(df['settlementDate']) + pd.to_timedelta((df['settlementPeriod'] - 1) * 30, unit='minutes')
            df = df.set_index('datetime').sort_index()
        
        # Get price column
        price_col = 'price' if 'price' in df.columns else df.select_dtypes(include=[np.number]).columns[0]
        prices = df[price_col].dropna()
        
        if len(prices) < 10:
            st.error("Insufficient price data for analysis. Please try a different date range.")
            st.stop()
            
    except Exception as e:
        st.error(f"Error processing data: {str(e)}")
        st.stop()
    
    # Calculate returns and metrics
    returns = calculate_returns(prices, return_type)
    metrics = calculate_volatility_metrics(returns)
    tests = perform_statistical_tests(returns)
    
    # Main dashboard
    st.markdown("## 📊 Data Overview")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Data Points", len(prices))
    with col2:
        st.metric("Date Range", f"{len((prices.index.max() - prices.index.min()).days)} days")
    with col3:
        st.metric("Price Range", f"£{prices.min():.2f} - £{prices.max():.2f}")
    with col4:
        st.metric("Latest Price", f"£{prices.iloc[-1]:.2f}")
    
    # Volatility Dashboard
    st.markdown("## 📈 Volatility Analysis")
    volatility_fig = create_volatility_dashboard(prices, returns, metrics, tests)
    st.plotly_chart(volatility_fig, use_container_width=True)
    
    # Risk Metrics
    st.markdown("## ⚠️ Risk Metrics")
    risk_fig = create_risk_metrics_chart(metrics)
    st.plotly_chart(risk_fig, use_container_width=True)
    
    # Statistical Tests Results
    st.markdown("## 🔬 Statistical Tests")
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Stationarity (ADF Test)")
        if tests['is_stationary']:
            st.success(f"✅ Series is stationary (p-value: {tests['adf_pvalue']:.4f})")
        else:
            st.warning(f"⚠️ Series may not be stationary (p-value: {tests['adf_pvalue']:.4f})")
        
        st.markdown("### Normality (Jarque-Bera Test)")
        if tests['is_normal']:
            st.success(f"✅ Returns are normally distributed (p-value: {tests['jb_pvalue']:.4f})")
        else:
            st.warning(f"⚠️ Returns are not normally distributed (p-value: {tests['jb_pvalue']:.4f})")
    
    with col2:
        st.markdown("### ARCH Effects Test")
        if tests['has_arch_effects']:
            st.info(f"📊 ARCH effects detected (p-value: {tests['arch_pvalue']:.4f})")
            st.caption("This suggests volatility clustering - GARCH modeling recommended")
        else:
            st.success(f"✅ No significant ARCH effects (p-value: {tests['arch_pvalue']:.4f})")
    
    # Risk Summary Table
    st.markdown("### Risk Summary")
    risk_summary = pd.DataFrame({
        'Metric': ['Daily Volatility', 'Annualized Volatility', 'VaR (95%)', 'VaR (99%)', 
                   'CVaR (95%)', 'CVaR (99%)', 'Maximum Drawdown', 'Skewness', 'Excess Kurtosis'],
        'Value': [f"{metrics['volatility']*100:.2f}%", f"{metrics['annualized_vol']*100:.2f}%",
                  f"{metrics['var_95']*100:.2f}%", f"{metrics['var_99']*100:.2f}%",
                  f"{metrics['cvar_95']*100:.2f}%", f"{metrics['cvar_99']*100:.2f}%",
                  f"{metrics['max_drawdown']*100:.2f}%", f"{metrics['skewness']:.2f}",
                  f"{metrics['kurtosis']:.2f}"]
    })
    st.dataframe(risk_summary, use_container_width=True)
    
    # GARCH Modeling (if available)
    if ARCH_AVAILABLE and tests['has_arch_effects']:
        st.markdown("## 🎯 GARCH Volatility Models")
        
        with st.spinner("Fitting GARCH models..."):
            garch_models = fit_garch_models(returns)
        
        if garch_models:
            # Model comparison
            model_comparison = []
            for name, model in garch_models.items():
                model_comparison.append({
                    'Model': name,
                    'AIC': model.aic,
                    'BIC': model.bic,
                    'Log-Likelihood': model.loglikelihood
                })
            
            comparison_df = pd.DataFrame(model_comparison)
            st.markdown("### Model Comparison")
            st.dataframe(comparison_df.round(4), use_container_width=True)
            
            # Best model
            best_model_name = comparison_df.loc[comparison_df['AIC'].idxmin(), 'Model']
            best_model = garch_models[best_model_name]
            st.success(f"🏆 Best model: {best_model_name} (lowest AIC)")
            
            # Volatility forecast
            st.markdown("### Volatility Forecast")
            try:
                forecast = best_model.forecast(horizon=5)
                forecast_vol = np.sqrt(forecast.variance.values[-1, :]) / 100
                
                forecast_df = pd.DataFrame({
                    'Horizon': range(1, 6),
                    'Volatility Forecast (%)': forecast_vol * 100
                })
                st.dataframe(forecast_df.round(4), use_container_width=True)
                
                # Monte Carlo VaR with GARCH forecast
                st.markdown("### Monte Carlo VaR (using GARCH forecast)")
                mc_var_95 = monte_carlo_var(returns, forecast_vol[0], confidence_level=0.05)
                mc_var_99 = monte_carlo_var(returns, forecast_vol[0], confidence_level=0.01)
                
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("MC VaR (95%)", f"{mc_var_95*100:.2f}%")
                with col2:
                    st.metric("MC VaR (99%)", f"{mc_var_99*100:.2f}%")
                    
            except Exception as e:
                st.warning(f"Could not generate forecast: {str(e)}")
    
    else:
        if not ARCH_AVAILABLE:
            st.info("💡 Install the 'arch' package to enable GARCH modeling: `pip install arch`")
        else:
            st.info("💡 No significant ARCH effects detected. GARCH modeling may not be necessary.")
    
    # Export options
    st.markdown("## 📥 Export Data")
    if st.button("Download Risk Analysis Report"):
        # Create export data
        export_data = {
            'prices': prices,
            'returns': returns,
            'metrics': metrics,
            'tests': tests
        }
        
        # Convert to CSV format for download
        export_df = pd.DataFrame({
            'datetime': prices.index,
            'price': prices.values,
            'returns': returns.reindex(prices.index).fillna(0).values
        })
        
        csv = export_df.to_csv(index=False)
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name=f"volatility_analysis_{start_date}_{end_date}.csv",
            mime="text/csv"
        )
