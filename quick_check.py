import requests

# Replace with your actual API URLs
TSERIES_URL = "https://www.alphavantage.co/query"
HIST_URL = "https://www.alphavantage.co/query"

API_KEY_TSERIES = 'WSUGEIUEWZZ15L4J'
API_KEY_HIST = '6ISW2VWZ0LP1L4TV'

def check_api(api_key, symbol="AAPL", function="TIME_SERIES_DAILY"):
    params = {
        "function": function,
        "symbol": symbol,
        "apikey": api_key,
        "outputsize": "compact"  # or 'full' for full history
    }
    try:
        response = requests.get(TSERIES_URL, params=params, timeout=10)
        data = response.json()
        if data:
            print(f"API key {api_key} returned data keys:", list(data.keys()))
        else:
            print(f"API key {api_key} returned no data (None or empty).")
    except Exception as e:
        print(f"Error calling API key {api_key}: {e}")

# Quick checks
print("Checking Time Series API key:")
check_api(API_KEY_TSERIES)

print("\nChecking Historical API key:")
check_api(API_KEY_HIST)
