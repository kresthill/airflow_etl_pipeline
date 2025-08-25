# fetch.py
import os
from dotenv import load_dotenv
from .utils import fetch_data

load_dotenv(".env")
API_KEY_T = os.getenv("API_KEY_TSERIES")
API_KEY_H = os.getenv("API_KEY_HIST")

def build_intraday_url(symbol: str, interval: str = "5min"):
    return (
        "https://www.alphavantage.co/query"
        f"?function=TIME_SERIES_INTRADAY&symbol={symbol}"
        f"&interval={interval}&apikey={API_KEY_T}"
    )

def build_daily_url(symbol: str):
    return (
        "https://www.alphavantage.co/query"
        f"?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={API_KEY_H}"
    )

def fetch_time_series(symbol: str, interval: str):
    api_url = f"https://example.com/timeseries/{symbol}?interval={interval}"
    return fetch_data(api_url)

def fetch_historical(symbol: str):
    api_url = f"https://example.com/historical/{symbol}"
    return fetch_data(api_url)
