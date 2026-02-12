"""
Zillow Data Loading Module

This module handles loading extracted Zillow housing data into PostgreSQL.
Supports both raw data loading and incremental updates.
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from typing import Optional


def get_db_connection_string() -> str:
    """
    Build PostgreSQL connection string from environment variables.

    Returns:
        str: SQLAlchemy connection string
    """
    host = os.getenv("PIPE_DB_HOST", "localhost")
    port = os.getenv("PIPE_DB_PORT", "5434")
    database = os.getenv("PIPE_DB_NAME", "pipeline")
    user = os.getenv("PIPE_DB_USER", "pipeline")
    password = os.getenv("PIPE_DB_PASSWORD", "pipeline")

    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"


def melt_zillow_dataframe(
    df: pd.DataFrame,
    value_name: str,
    id_vars: Optional[list] = None
) -> pd.DataFrame:
    """
    Convert wide-format Zillow data to long format.

    Args:
        df: Wide-format DataFrame with date columns
        value_name: Name for the value column (e.g., 'home_value', 'rent_value')
        id_vars: List of ID columns to preserve (if None, auto-detected)

    Returns:
        pd.DataFrame: Long-format DataFrame with one row per region per date
    """
    if id_vars is None:
        # Typical Zillow columns: RegionID, SizeRank, RegionName, RegionType, StateName
        id_vars = [col for col in df.columns if not col.replace('-', '').replace('/', '').isdigit()
                   and not (len(col) >= 7 and col[4] == '-')]

    # unpivot all date columns into rows
    df_long = df.melt(
        id_vars=id_vars,
        var_name='date',
        value_name=value_name
    )

    # Standardize column names to match our raw table schema
    column_mapping = {
        'RegionID': 'region_id',
        'SizeRank': 'size_rank',
        'RegionName': 'region_name',
        'RegionType': 'region_type',
        'StateName': 'state_name'
    }
    df_long = df_long.rename(columns=column_mapping)

    # Convert date string to datetime
    df_long['date'] = pd.to_datetime(df_long['date'])

    # Drop rows with null values (missing data points)
    df_long = df_long.dropna(subset=[value_name])

    return df_long


def load_to_postgres(
    df: pd.DataFrame,
    table_name: str,
    if_exists: str = 'replace'
) -> int:
    """
    Load DataFrame into PostgreSQL table.

    Args:
        df: DataFrame to load
        table_name: Target table name
        if_exists: How to behave if table exists ('replace', 'append', 'fail')

    Returns:
        int: Number of rows loaded
    """
    conn_string = get_db_connection_string()
    engine = create_engine(conn_string)

    print(f"Loading {len(df)} rows into {table_name}...")

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists=if_exists,
        index=False,
        method='multi',
        chunksize=1000
    )

    print(f"✓ Loaded {len(df)} rows into {table_name}")
    engine.dispose()

    return len(df)


def load_zhvi(df_zhvi: pd.DataFrame) -> int:
    """
    Transform and load ZHVI data into raw_zhvi table.

    Args:
        df_zhvi: Wide-format ZHVI DataFrame

    Returns:
        int: Number of rows loaded
    """
    print("Transforming ZHVI data from wide to long format...")
    df_long = melt_zillow_dataframe(df_zhvi, value_name='home_value')
    print(f"✓ Melted to {len(df_long)} rows")

    return load_to_postgres(df_long, 'raw_zhvi', if_exists='replace')


def load_zori(df_zori: pd.DataFrame) -> int:
    """
    Transform and load ZORI data into raw_zori table.

    Args:
        df_zori: Wide-format ZORI DataFrame

    Returns:
        int: Number of rows loaded
    """
    print("Transforming ZORI data from wide to long format...")
    df_long = melt_zillow_dataframe(df_zori, value_name='rent_value')
    print(f"✓ Melted to {len(df_long)} rows")

    return load_to_postgres(df_long, 'raw_zori', if_exists='replace')


def execute_sql_file(sql_file_path: str) -> None:
    """
    Execute a SQL script file against the pipeline database.

    Args:
        sql_file_path: Path to SQL file
    """
    print(f"Executing SQL script: {sql_file_path}")

    with open(sql_file_path, 'r') as f:
        sql_script = f.read()

    conn_string = get_db_connection_string()
    engine = create_engine(conn_string)

    with engine.connect() as conn:
        # Split by semicolon and execute each statement
        statements = [s.strip() for s in sql_script.split(';') if s.strip()]
        for statement in statements:
            if statement:
                conn.execute(text(statement))
        conn.commit()

    print(f"✓ Executed {sql_file_path}")
    engine.dispose()
