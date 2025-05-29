import streamlit as st
import pandas as pd
import importlib
from prophet import Prophet

# Forecasting page

def render(df: pd.DataFrame, **kwargs):
    st.header("Forecasting")
    if df is None or df.empty:
        st.warning("No data for forecasting.")
        return
    # Prepare data for Prophet
    if 'date' not in df.columns or 'price_€/MWh' not in df.columns:
        st.error("Data must contain 'date' and 'price_€/MWh' columns for forecasting.")
        return

    df_prophet = df[['date', 'price_€/MWh']].rename(columns={'date': 'ds', 'price_€/MWh': 'y'})
    # Date range selection
    min_date, max_date = df_prophet['ds'].min(), df_prophet['ds'].max()
    date_range = st.date_input("Select date range", [min_date, max_date])
    # Ensure two dates are selected
    if not isinstance(date_range, (list, tuple)) or len(date_range) != 2:
        st.error("Please select a start and end date.")
        return
    selected_start, selected_end = date_range[0], date_range[1]
    # Convert to pandas Timestamps for consistent indexing
    selected_start = pd.to_datetime(selected_start)
    selected_end = pd.to_datetime(selected_end)

    if selected_start > selected_end:
        st.error("Start date must be before end date.")
        return

    period = st.number_input("Forecast horizon (days)", min_value=1, value=7)
    # Model selection
    model_option = st.selectbox("Choose forecasting model", ["Prophet", "ARIMA"])

    if model_option == "Prophet":
        train_df = df_prophet[(df_prophet['ds'] >= pd.to_datetime(selected_start)) &
                              (df_prophet['ds'] <= pd.to_datetime(selected_end))]
        m = Prophet()
        m.fit(train_df)
        future = m.make_future_dataframe(periods=period)
        forecast = m.predict(future)

        st.subheader("Prophet Forecast")
        fig1 = m.plot(forecast)
        st.pyplot(fig1)

        st.subheader("Forecast Data")
        st.dataframe(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(10))
    else:
        # ARIMA forecasting on daily aggregated data
        try:
            statsmod = importlib.import_module("statsmodels.tsa.arima.model")
            ARIMA = getattr(statsmod, "ARIMA", None)
            if ARIMA is None:
                raise ImportError
        except ImportError:
            st.error("ARIMA model not available. Please install statsmodels.")
            return
        # Aggregate to daily frequency
        ts = df_prophet.set_index('ds')['y']
        ts_daily = ts.resample('D').mean()
        ts_train = ts_daily.loc[selected_start:selected_end]

        st.subheader("ARIMA Settings")
        p = st.number_input("AR order (p)", min_value=0, value=5)
        d = st.number_input("Difference order (d)", min_value=0, value=1)
        q = st.number_input("MA order (q)", min_value=0, value=0)

        # Fit ARIMA model
        model = ARIMA(ts_train, order=(p, d, q))
        model_fit = model.fit()
        forecast_res = model_fit.get_forecast(steps=period)
        forecast_df = forecast_res.summary_frame()

        # Plot actual vs forecast side by side
        combined = pd.concat([
            ts_daily.rename('actual'),
            forecast_df['mean'].rename('forecast')
        ], axis=1)
        st.subheader("ARIMA Forecast")
        st.line_chart(combined)

        st.subheader("Forecast Data")
        st.dataframe(forecast_df[['mean', 'mean_ci_lower', 'mean_ci_upper']].rename(
            columns={'mean':'yhat','mean_ci_lower':'yhat_lower','mean_ci_upper':'yhat_upper'}
        ).tail(10))
