# extract.py
import pandas as pd

def extract_time_series(api_data: dict) -> pd.DataFrame:
    # Alpha Vantage key e.g. "Time Series (5min)"
    key = next((k for k in api_data.keys() if "Time Series (" in k), None)
    if not key:
        raise ValueError("Time series key not found in API response.")
    raw = api_data[key]
    df = pd.DataFrame(raw).T
    # Columns in correct order
    df.columns = ["Open", "High", "Low", "Close", "Volume"]
    df.index.name = "DateTime"
    return df.reset_index()

def extract_historical(api_data: dict) -> pd.DataFrame:
    key = "Time Series (Daily)"
    if key not in api_data:
        # Sometimes the key has slight variations; try best-effort find
        key = next((k for k in api_data.keys() if "Time Series (Daily)" in k), None)
    if not key:
        raise ValueError("Daily series key not found in API response.")
    raw = api_data[key]
    df = pd.DataFrame(raw).T
    df.columns = ["Open", "High", "Low", "Close", "Volume"]
    df.index.name = "DateTime"
    return df.reset_index()
