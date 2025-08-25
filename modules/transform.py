# transform.py
import pandas as pd

def _clean(df: pd.DataFrame) -> pd.DataFrame:
    # Debug: show initial dataframe info
    print("\n[DEBUG] Incoming DataFrame columns:", df.columns.tolist())
    print("[DEBUG] First 5 rows before cleaning:\n", df.head(), "\n")

    # Normalize column names to standard format
    rename_map = {
        "1. open": "Open",
        "2. high": "High",
        "3. low": "Low",
        "4. close": "Close",
        "5. volume": "Volume",
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume",
    }
    df = df.rename(columns=rename_map)

    # Handle flexible datetime column names
    if "DateTime" in df.columns:
        dt_col = "DateTime"
    elif "date" in df.columns:
        dt_col = "date"
    elif "timestamp" in df.columns:
        dt_col = "timestamp"
    else:
        # If no explicit date column, assume index is the datetime
        df = df.reset_index()
        dt_col = df.columns[0]

    # Standardize to DateTime column
    df["DateTime"] = pd.to_datetime(df[dt_col], utc=True, errors="coerce")

    # Cast numeric fields if present
    numeric_cols = ["Open", "High", "Low", "Close", "Volume"]
    for col in numeric_cols:
        if col in df.columns:
            if col == "Volume":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            else:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float")

    # Drop invalids and duplicates
    df = (
        df.dropna(subset=["DateTime"])
          .drop_duplicates(subset=["DateTime"])
          .sort_values("DateTime")
    )

    # Debug: show after cleaning
    print("[DEBUG] Cleaned DataFrame columns:", df.columns.tolist())
    print("[DEBUG] First 5 rows after cleaning:\n", df.head(), "\n")
    
    df.columns = [c.lower() for c in df.columns]

    return df


def transform_time_series(df: pd.DataFrame) -> pd.DataFrame:
    return _clean(df)

def transform_historical(df: pd.DataFrame) -> pd.DataFrame:
    # Historical dates come as midnight UTC; still store as TIMESTAMP for uniformity
    return _clean(df)
