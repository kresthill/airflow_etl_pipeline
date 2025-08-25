# modules/load.py
import pandas as pd
from sqlalchemy import text
from airflow.providers.postgres.hooks.postgres import PostgresHook


# -------------------------
# Helper: Get Engine
# -------------------------
def get_engine(conn_id: str = "stocks_db"):
    """
    Get SQLAlchemy engine from an Airflow Postgres connection.
    """
    hook = PostgresHook(postgres_conn_id=conn_id)
    return hook.get_sqlalchemy_engine()


# -------------------------
# Smart Upsert Helper - Detects Table Schema
# -------------------------
def _upsert(df: pd.DataFrame, table: str, conn_id: str = "stocks_db"):
    """
    Upsert stock data into the given table.
    Automatically detects whether table uses 'date' or 'datetime' column.
    """
    if df.empty:
        print(f"[DEBUG] No rows to insert into {table}")
        return 0

    engine = get_engine(conn_id)

    # First, detect the actual table schema
    with engine.begin() as conn:
        schema_query = text("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = :table_name 
            ORDER BY ordinal_position
        """)
        schema_result = conn.execute(schema_query, {"table_name": table})
        columns_info = {row[0]: row[1] for row in schema_result}
        print(f"[DEBUG] Detected {table} schema: {columns_info}")

    # Determine date column name from actual table schema
    date_col_name = None
    if "date" in columns_info:
        date_col_name = "date"
        date_col_type = "DATE"
    elif "datetime" in columns_info:
        date_col_name = "datetime"
        date_col_type = "TIMESTAMP"
    else:
        raise ValueError(f"Table {table} doesn't have a 'date' or 'datetime' column. Available: {list(columns_info.keys())}")

    print(f"[DEBUG] Using {date_col_name} column with type {date_col_type}")

    # Prepare DataFrame to match table schema
    df_prepared = df.copy()
    
    # Rename date column to match table schema and handle data types
    if "date" in df_prepared.columns and date_col_name == "datetime":
        # Convert date back to proper datetime for timestamp columns
        df_prepared["datetime"] = pd.to_datetime(df_prepared["date"], utc=True)
        df_prepared = df_prepared.drop(columns=["date"])
    elif "datetime" in df_prepared.columns and date_col_name == "date":
        # Convert datetime to date for date columns
        df_prepared["date"] = pd.to_datetime(df_prepared["datetime"]).dt.date
        df_prepared = df_prepared.drop(columns=["datetime"])
    elif date_col_name == "datetime" and "datetime" in df_prepared.columns:
        # Ensure datetime is timezone-aware for PostgreSQL
        if not pd.api.types.is_datetime64tz_dtype(df_prepared["datetime"]):
            df_prepared["datetime"] = pd.to_datetime(df_prepared["datetime"], utc=True)
    
    # Ensure numeric columns are proper types
    numeric_cols = ["open", "high", "low", "close"]
    for col in numeric_cols:
        if col in df_prepared.columns:
            df_prepared[col] = pd.to_numeric(df_prepared[col], errors="coerce").astype("float64")
    
    if "volume" in df_prepared.columns:
        # Convert to regular int64 for better PostgreSQL compatibility
        df_prepared["volume"] = pd.to_numeric(df_prepared["volume"], errors="coerce").fillna(0).astype("int64")
    
    print(f"[DEBUG] DataFrame after type preparation:\n{df_prepared.dtypes}")
    print(f"[DEBUG] Sample prepared data:\n{df_prepared.head()}")

    # Check required columns based on actual table schema
    required = ["symbol", date_col_name, "open", "high", "low", "close", "volume"]
    missing = [c for c in required if c not in df_prepared.columns]
    if missing:
        available = list(df_prepared.columns)
        raise ValueError(f"Missing columns for {table}: {missing}. Available: {available}")

    # Show what we're about to insert
    print(f"[DEBUG] About to upsert {len(df_prepared)} rows into {table}")
    print(f"[DEBUG] Data sample:\n{df_prepared.head()}")
    print(f"[DEBUG] Data types:\n{df_prepared.dtypes}")

    tmp_table = f"tmp_{table}_{int(pd.Timestamp.now().timestamp())}"
    
    try:
        with engine.begin() as conn:
            # Create temp table that matches the actual table structure
            if date_col_type == "DATE":
                temp_schema = f'''
                CREATE TEMP TABLE "{tmp_table}" (
                    symbol TEXT NOT NULL,
                    {date_col_name} DATE NOT NULL,
                    open NUMERIC,
                    high NUMERIC,
                    low NUMERIC,
                    close NUMERIC,
                    volume BIGINT
                ) ON COMMIT DROP;
                '''
            else:  # TIMESTAMP
                temp_schema = f'''
                CREATE TEMP TABLE "{tmp_table}" (
                    symbol TEXT NOT NULL,
                    {date_col_name} TIMESTAMP NOT NULL,
                    open NUMERIC,
                    high NUMERIC,
                    low NUMERIC,
                    close NUMERIC,
                    volume BIGINT
                ) ON COMMIT DROP;
                '''
            
            conn.execute(text(temp_schema))
            
            # Insert into temp table with proper data type handling
            df_to_insert = df_prepared[required].copy()
            
            # Handle datetime conversion for PostgreSQL
            if date_col_name == "datetime" and date_col_name in df_to_insert.columns:
                # Ensure datetime is timezone-aware for PostgreSQL timestamp with time zone
                if not df_to_insert[date_col_name].dt.tz:
                    df_to_insert[date_col_name] = pd.to_datetime(df_to_insert[date_col_name], utc=True)
                print(f"[DEBUG] DateTime column after conversion: {df_to_insert[date_col_name].dtype}")
                print(f"[DEBUG] Sample datetime values: {df_to_insert[date_col_name].head().tolist()}")
            
            # Debug: Show exactly what we're trying to insert
            print(f"[DEBUG] Columns to insert: {df_to_insert.columns.tolist()}")
            print(f"[DEBUG] Data types going to temp table:\n{df_to_insert.dtypes}")
            print(f"[DEBUG] Sample row for insertion:\n{df_to_insert.iloc[0].to_dict()}")
            
            # Use explicit SQL insertion instead of to_sql() to avoid silent failures
            print("[DEBUG] Using explicit SQL insertion instead of to_sql()")
            
            inserted_count = 0
            for idx, row in df_to_insert.iterrows():
                try:
                    # Convert row to dict and handle datetime properly
                    row_dict = row.to_dict()
                    
                    # Convert pandas Timestamp to Python datetime for PostgreSQL
                    if date_col_name in row_dict and hasattr(row_dict[date_col_name], 'to_pydatetime'):
                        row_dict[date_col_name] = row_dict[date_col_name].to_pydatetime()
                    
                    placeholders = ', '.join([f':{col}' for col in required])
                    insert_sql = f'INSERT INTO "{tmp_table}" ({", ".join(required)}) VALUES ({placeholders})'
                    
                    if idx == 0:  # Log first insert for debugging
                        print(f"[DEBUG] First insert SQL: {insert_sql}")
                        print(f"[DEBUG] First row params: {row_dict}")
                    
                    conn.execute(text(insert_sql), row_dict)
                    inserted_count += 1
                    
                    if idx == 0:
                        print(f"[DEBUG] First row inserted successfully")
                    elif idx % 20 == 0:  # Log progress every 20 rows
                        print(f"[DEBUG] Inserted {inserted_count} rows so far...")
                        
                except Exception as insert_error:
                    print(f"[ERROR] Failed to insert row {idx}: {insert_error}")
                    print(f"[ERROR] Row data: {row_dict}")
                    raise
            
            print(f"[DEBUG] Manual insertion completed: {inserted_count} rows")
            
            # Check temp table contents
            temp_count = conn.execute(text(f'SELECT COUNT(*) FROM "{tmp_table}"')).scalar()
            print(f"[DEBUG] Inserted {temp_count} rows into temp table")

            # Upsert from temp into target (using detected column name)
            merge_sql = f"""
            INSERT INTO "{table}" (symbol, {date_col_name}, open, high, low, close, volume)
            SELECT symbol, {date_col_name}, open, high, low, close, volume FROM "{tmp_table}"
            ON CONFLICT (symbol, {date_col_name})
            DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume;
            """
            res = conn.execute(text(merge_sql))
            rows_affected = res.rowcount or 0
            print(f"[DEBUG] Upserted {rows_affected} rows into {table}")
            
            # Verify final count in target table
            final_count = conn.execute(text(f'SELECT COUNT(*) FROM "{table}" WHERE symbol = :symbol'), {"symbol": df_prepared.iloc[0]["symbol"]}).scalar()
            print(f"[DEBUG] Total rows in {table} for symbol {df_prepared.iloc[0]['symbol']}: {final_count}")
            
            return rows_affected
            
    except Exception as e:
        print(f"[ERROR] Upsert failed: {e}")
        import traceback
        print(f"[ERROR] Traceback: {traceback.format_exc()}")
        raise


# -------------------------
# Public Loaders
# -------------------------
def load_time_series(df, symbol, conn_id="stocks_db"):
    """Load cleaned DataFrame into time_series table with schema enforcement."""
    if df.empty:
        print("[DEBUG] Empty DataFrame provided to load_time_series")
        return 0
        
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    
    print(f"[DEBUG] load_time_series received DataFrame with columns: {df.columns.tolist()}")
    print(f"[DEBUG] DataFrame shape: {df.shape}")

    # Handle datetime -> date conversion
    datetime_col = None
    if "datetime" in df.columns:
        datetime_col = "datetime"
    elif "timestamp" in df.columns:
        datetime_col = "timestamp"
    elif "date" in df.columns:
        datetime_col = "date"
    else:
        raise ValueError(
            f"Input DataFrame must contain a datetime column. "
            f"Available columns: {list(df.columns)}"
        )

    # Convert to date column for database schema
    df["date"] = pd.to_datetime(df[datetime_col], utc=True).dt.date
    
    # Clean up - remove the original datetime column if it's different from 'date'
    if datetime_col != "date" and datetime_col in df.columns:
        df = df.drop(columns=[datetime_col])

    # Add/standardize symbol
    if "symbol" not in df.columns:
        df["symbol"] = symbol.upper()
    else:
        df["symbol"] = df["symbol"].str.upper()

    # Ensure numeric types
    numeric_cols = ["open", "high", "low", "close"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    
    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").astype("Int64")

    # Remove rows with null dates or all null numeric values
    df = df.dropna(subset=["date"])
    df = df.dropna(subset=["open", "high", "low", "close"], how="all")

    print(f"[DEBUG] Final DataFrame for time_series: {len(df)} rows")
    if not df.empty:
        print(f"[DEBUG] Sample data:\n{df.head()}")

    inserted = _upsert(df, "time_series", conn_id)
    print(f"[DEBUG] load_time_series completed: {inserted} rows for {symbol}")
    return inserted


def load_historical(df: pd.DataFrame, symbol: str, conn_id: str = "stocks_db"):
    """Load cleaned DataFrame into historical table with schema enforcement."""
    if df.empty:
        print("[DEBUG] Empty DataFrame provided to load_historical")
        return 0
        
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    
    print(f"[DEBUG] load_historical received DataFrame with columns: {df.columns.tolist()}")
    print(f"[DEBUG] DataFrame shape: {df.shape}")

    # Handle datetime -> date conversion
    datetime_col = None
    if "datetime" in df.columns:
        datetime_col = "datetime"
    elif "timestamp" in df.columns:
        datetime_col = "timestamp"
    elif "date" in df.columns:
        datetime_col = "date"
    else:
        raise ValueError(
            f"Input DataFrame must contain a datetime column. "
            f"Available columns: {list(df.columns)}"
        )

    # Convert to date column for database schema
    df["date"] = pd.to_datetime(df[datetime_col], utc=True).dt.date
    
    # Clean up - remove the original datetime column if it's different from 'date'
    if datetime_col != "date" and datetime_col in df.columns:
        df = df.drop(columns=[datetime_col])

    # Force symbol column
    df["symbol"] = symbol.upper()
    
    # Ensure numeric types
    numeric_cols = ["open", "high", "low", "close"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    
    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").astype("Int64")

    # Remove rows with null dates or all null numeric values
    df = df.dropna(subset=["date"])
    df = df.dropna(subset=["open", "high", "low", "close"], how="all")

    print(f"[DEBUG] Final DataFrame for historical: {len(df)} rows")
    if not df.empty:
        print(f"[DEBUG] Sample data:\n{df.head()}")

    inserted = _upsert(df, "historical", conn_id)
    print(f"[DEBUG] load_historical completed: {inserted} rows for {symbol}")
    return inserted