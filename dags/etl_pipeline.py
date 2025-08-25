# dags/stock_etl_dag.py
from __future__ import annotations
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.postgres.operators.postgres import PostgresOperator
import sys, os
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]  # one level up from dags/
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import pipeline modules
from modules.extract import extract_time_series, extract_historical
from modules.transform import transform_time_series, transform_historical
from modules.load import load_time_series, load_historical
from modules.utils import fetch_data, API_KEY_TSERIES, API_KEY_HIST

DEFAULT_SYMBOL = os.getenv("SYMBOL", "IBM")
DEFAULT_INTERVAL = os.getenv("INTERVAL", "DAILY")


@dag(
    dag_id="stocks_data_etl",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "owner": "data-eng",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    params={
        "symbol": Param(DEFAULT_SYMBOL, type="string"),
        "interval": Param(DEFAULT_INTERVAL, type="string"),
    },
    tags=["stocks", "alpha_vantage", "etl", "postgres"],
)
def stocks_data_etl():

    # --------------------------
    # Step 0: Ensure tables exist with UNIQUE constraints
    # --------------------------
    init_db = PostgresOperator(
        task_id="init_db",
        postgres_conn_id="stocks_db",
        sql="""
        CREATE TABLE IF NOT EXISTS time_series (
            id SERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            date DATE NOT NULL,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume BIGINT,
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(symbol, date)
        );

        CREATE TABLE IF NOT EXISTS historical (
            id SERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            date DATE NOT NULL,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume BIGINT,
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(symbol, date)
        );
        """,
    )

    # --------------------------
    # Fetch with better error handling
    # --------------------------
    @task
    def fetch_ts(symbol: str, interval: str) -> dict:
        """Fetch time series data with comprehensive error handling"""
        api_url = (
            f"https://www.alphavantage.co/query?"
            f"function=TIME_SERIES_{interval.upper()}&symbol={symbol}&apikey={API_KEY_TSERIES}"
        )
        print(f"[DEBUG] Fetching time series from: {api_url}")
        
        data = fetch_data(api_url)
        if not data:
            print("[ERROR] No data returned from time series API")
            return {}
        
        print(f"[DEBUG] API Response keys: {list(data.keys())}")
        
        # Check for API errors
        if "Error Message" in data:
            print(f"[ERROR] API Error: {data['Error Message']}")
            return {}
        if "Note" in data:
            print(f"[WARNING] API Note: {data['Note']}")
            
        return data

    @task
    def fetch_hist(symbol: str) -> dict:
        """Fetch historical data with comprehensive error handling"""
        api_url = (
            f"https://www.alphavantage.co/query?"
            f"function=TIME_SERIES_DAILY&symbol={symbol}&apikey={API_KEY_HIST}"
        )
        print(f"[DEBUG] Fetching historical from: {api_url}")
        
        data = fetch_data(api_url)
        if not data:
            print("[ERROR] No data returned from historical API")
            return {}
        
        print(f"[DEBUG] API Response keys: {list(data.keys())}")
        
        # Check for API errors
        if "Error Message" in data:
            print(f"[ERROR] API Error: {data['Error Message']}")
            return {}
        if "Note" in data:
            print(f"[WARNING] API Note: {data['Note']}")
            
        return data

    # --------------------------
    # Extract with better debugging
    # --------------------------
    @task
    def extract_ts(raw: dict) -> str:
        """Extract time series data with detailed logging"""
        print(f"[DEBUG] Extract TS input keys: {list(raw.keys()) if raw else 'Empty'}")
        
        try:
            if not raw:
                print("[WARNING] No raw data to extract")
                df = pd.DataFrame()
            else:
                df = extract_time_series(raw)
                print(f"[DEBUG] Extracted {len(df)} time series rows")
                if not df.empty:
                    print(f"[DEBUG] Columns: {df.columns.tolist()}")
                    print(f"[DEBUG] Sample data:\n{df.head()}")
        except Exception as e:
            print(f"[ERROR] Error extracting time series: {e}")
            import traceback
            print(f"[ERROR] Traceback: {traceback.format_exc()}")
            df = pd.DataFrame()
            
        path = f"/tmp/ts_{datetime.utcnow().timestamp()}.parquet"
        df.to_parquet(path, index=False)
        print(f"[DEBUG] Saved {len(df)} rows to {path}")
        return path

    @task
    def extract_hist(raw: dict) -> str:
        """Extract historical data with detailed logging"""
        print(f"[DEBUG] Extract HIST input keys: {list(raw.keys()) if raw else 'Empty'}")
        
        try:
            if not raw:
                print("[WARNING] No raw data to extract")
                df = pd.DataFrame()
            else:
                df = extract_historical(raw)
                print(f"[DEBUG] Extracted {len(df)} historical rows")
                if not df.empty:
                    print(f"[DEBUG] Columns: {df.columns.tolist()}")
                    print(f"[DEBUG] Sample data:\n{df.head()}")
        except Exception as e:
            print(f"[ERROR] Error extracting historical: {e}")
            import traceback
            print(f"[ERROR] Traceback: {traceback.format_exc()}")
            df = pd.DataFrame()
            
        path = f"/tmp/hist_{datetime.utcnow().timestamp()}.parquet"
        df.to_parquet(path, index=False)
        print(f"[DEBUG] Saved {len(df)} rows to {path}")
        return path

    # --------------------------
    # Transform with better debugging
    # --------------------------
    @task
    def transform_ts(path: str) -> str:
        """Transform time series data with detailed logging"""
        df = pd.read_parquet(path)
        print(f"[DEBUG] Transform TS loaded {len(df)} rows from {path}")
        
        if not df.empty:
            try:
                df = transform_time_series(df)
                print(f"[DEBUG] Transformed to {len(df)} rows")
                if not df.empty:
                    print(f"[DEBUG] Final columns: {df.columns.tolist()}")
                    print(f"[DEBUG] Final sample:\n{df.head()}")
            except Exception as e:
                print(f"[ERROR] Transform error: {e}")
                import traceback
                print(f"[ERROR] Traceback: {traceback.format_exc()}")
                df = pd.DataFrame()
        
        path_out = f"/tmp/ts_clean_{datetime.utcnow().timestamp()}.parquet"
        df.to_parquet(path_out, index=False)
        print(f"[DEBUG] Saved {len(df)} transformed rows to {path_out}")
        return path_out

    @task
    def transform_hist(path: str) -> str:
        """Transform historical data with detailed logging"""
        df = pd.read_parquet(path)
        print(f"[DEBUG] Transform HIST loaded {len(df)} rows from {path}")
        
        if not df.empty:
            try:
                df = transform_historical(df)
                print(f"[DEBUG] Transformed to {len(df)} rows")
                if not df.empty:
                    print(f"[DEBUG] Final columns: {df.columns.tolist()}")
                    print(f"[DEBUG] Final sample:\n{df.head()}")
            except Exception as e:
                print(f"[ERROR] Transform error: {e}")
                import traceback
                print(f"[ERROR] Traceback: {traceback.format_exc()}")
                df = pd.DataFrame()
                
        path_out = f"/tmp/hist_clean_{datetime.utcnow().timestamp()}.parquet"
        df.to_parquet(path_out, index=False)
        print(f"[DEBUG] Saved {len(df)} transformed rows to {path_out}")
        return path_out

    # --------------------------
    # Load with better debugging
    # --------------------------
    @task
    def load_ts(path: str, symbol: str) -> int:
        """Load time series data with detailed logging"""
        df = pd.read_parquet(path)
        print(f"[DEBUG] Load TS loaded {len(df)} rows from {path}")
        
        if df.empty:
            print("[WARNING] No time series data to load")
            return 0
            
        try:
            rows_inserted = load_time_series(df, symbol, conn_id="stocks_db")
            print(f"[DEBUG] Successfully loaded {rows_inserted} time series rows")
            return rows_inserted
        except Exception as e:
            print(f"[ERROR] Load time series error: {e}")
            import traceback
            print(f"[ERROR] Traceback: {traceback.format_exc()}")
            return 0

    @task
    def load_hist(path: str, symbol: str) -> int:
        """Load historical data with detailed logging"""
        df = pd.read_parquet(path)
        print(f"[DEBUG] Load HIST loaded {len(df)} rows from {path}")
        
        if df.empty:
            print("[WARNING] No historical data to load")
            return 0
            
        try:
            rows_inserted = load_historical(df, symbol, conn_id="stocks_db")
            print(f"[DEBUG] Successfully loaded {rows_inserted} historical rows")
            return rows_inserted
        except Exception as e:
            print(f"[ERROR] Load historical error: {e}")
            import traceback
            print(f"[ERROR] Traceback: {traceback.format_exc()}")
            return 0
        
        
    # Add this debug task to your DAG for testing

    @task
    def debug_api_connection():
        """Debug task to test API connection and environment variables"""
        import os
        from modules.utils import API_KEY_TSERIES, API_KEY_HIST, fetch_data
        
        print(f"[DEBUG] API_KEY_TSERIES: '{API_KEY_TSERIES}'")
        print(f"[DEBUG] API_KEY_HIST: '{API_KEY_HIST}'")
        print(f"[DEBUG] Length of TSERIES key: {len(API_KEY_TSERIES) if API_KEY_TSERIES else 'None'}")
        print(f"[DEBUG] Length of HIST key: {len(API_KEY_HIST) if API_KEY_HIST else 'None'}")
        
        # Test API call
        test_url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey={API_KEY_HIST}"
        print(f"[DEBUG] Test URL: {test_url}")
        
        response = fetch_data(test_url)
        if response:
            print(f"[DEBUG] API Response keys: {list(response.keys())}")
            if "Error Message" in response:
                print(f"[ERROR] API Error: {response['Error Message']}")
            elif "Note" in response:
                print(f"[WARNING] API Note: {response['Note']}")
            else:
                print("[SUCCESS] API call successful!")
                # Show first few data points
                for key, value in response.items():
                    if isinstance(value, dict):
                        print(f"[DEBUG] {key}: {len(value)} data points")
                        if len(value) > 0:
                            first_key = list(value.keys())[0]
                            print(f"[DEBUG] Sample: {first_key} -> {value[first_key]}")
                        break
        else:
            print("[ERROR] No response from API")
        
        return "Debug complete"

# Add this to your DAG orchestration section:
# debug_task = debug_api_connection()

    # --------------------------
    # DAG Orchestration
    # --------------------------
    ts_raw = fetch_ts(symbol="{{ params.symbol }}", interval="{{ params.interval }}")
    h_raw = fetch_hist(symbol="{{ params.symbol }}")

    ts_ext = extract_ts(ts_raw)
    h_ext = extract_hist(h_raw)

    ts_trn = transform_ts(ts_ext)
    h_trn = transform_hist(h_ext)

    ts_ld = load_ts(ts_trn, symbol="{{ params.symbol }}")
    h_ld = load_hist(h_trn, symbol="{{ params.symbol }}")

    # Ensure database is initialized before any data operations
    init_db >> [ts_raw, h_raw]


etl_dag = stocks_data_etl()

