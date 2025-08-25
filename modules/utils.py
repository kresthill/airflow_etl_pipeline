# modules/utils.py
import requests
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load environment variables from .env
load_dotenv()

# Alpha Vantage API keys
API_KEY_TSERIES = os.getenv("API_KEY_TSERIES")
API_KEY_HIST = os.getenv("API_KEY_HIST")

def fetch_data(api_url: str):
    """
    Fetch JSON data from an API URL with timeout.
    """
    try:
        r = requests.get(api_url, timeout=60)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[fetch_data] Error: {e}")
        return None


def get_api_data(url: str):
    """
    Wrapper around fetch_data for backwards compatibility.
    """
    return fetch_data(url)


def get_engine():
    """
    Returns a SQLAlchemy engine.

    Priority:
    1. Airflow connection 'stocks_postgres' if running in Airflow.
    2. Local fallback using .env credentials.

    Handles both 'postgres://' and 'postgresql://' URIs and strips whitespace.
    """
    # Load environment variables in case running locally
    load_dotenv()

    # Try Airflow connection first
    try:
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection("stocks_db")
        uri = conn.get_uri()

        # Fix legacy postgres:// URI
        if uri.startswith("postgres://"):
            uri = uri.replace("postgres://", "postgresql+psycopg2://")

        # Ensure psycopg2 driver in URI
        if "postgresql+" in uri and "+psycopg2" not in uri:
            uri = uri.replace("postgresql://", "postgresql+psycopg2://")

        return create_engine(uri, pool_pre_ping=True)

    except Exception:
        # Local fallback to .env
        db_user = os.getenv("ADB_USER", "").strip()
        db_pass = os.getenv("ADB_PASSWORD", "").strip()
        db_host = os.getenv("ADB_HOST", "localhost").strip()
        db_port = os.getenv("ADB_PORT", "5432").strip()
        db_name = os.getenv("ADB_NAME", "").strip()

        if not all([db_user, db_pass, db_name]):
            raise RuntimeError(
                "No Airflow connection and missing DB creds in .env "
                "(ADB_USER/ADB_PASSWORD/ADB_HOST/ADB_PORT/ADB_NAME)."
            )

        uri = f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
        return create_engine(uri, pool_pre_ping=True)
