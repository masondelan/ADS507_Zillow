"""
Zillow Housing Data ETL Pipeline DAG

This DAG orchestrates the extraction, transformation, and loading of
Zillow housing market data including ZHVI (Home Values) and ZORI (Rentals).

Task graph:
    [extract_zhvi] ──► [load_zhvi_to_raw] ──┐
                                            ├──► [transform_to_staging] ──► [build_data_marts] ──► [data_quality_check]
    [extract_zori] ──► [load_zori_to_raw] ──┘
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

# Import ETL functions
from etl.extract import extract_zhvi as _extract_zhvi, extract_zori as _extract_zori
from etl.load import load_zhvi, load_zori, execute_sql_file


default_args = {
    "owner": "ads507",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="zillow_etl_pipeline",
    description="Zillow Housing Data ETL Pipeline",
    schedule_interval="@monthly",
    default_args=default_args,
    catchup=False,
)


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------


def extract_zhvi(**kwargs):
    """Extract ZHVI data from Zillow Research and push to XCom."""
    df = _extract_zhvi()
    # Store DataFrame in XCom for next task
    kwargs['ti'].xcom_push(key='zhvi_data', value=df.to_json())


def extract_zori(**kwargs):
    """Extract ZORI data from Zillow Research and push to XCom."""
    df = _extract_zori()
    # Store DataFrame in XCom for next task
    kwargs['ti'].xcom_push(key='zori_data', value=df.to_json())


def load_zhvi_to_raw(**kwargs):
    """Load ZHVI data into raw.raw_zhvi table."""
    import pandas as pd

    # Pull DataFrame from XCom
    zhvi_json = kwargs['ti'].xcom_pull(key='zhvi_data', task_ids='extract_zhvi')
    df = pd.read_json(zhvi_json)

    # Transform and load
    load_zhvi(df)


def load_zori_to_raw(**kwargs):
    """Load ZORI data into raw.raw_zori table."""
    import pandas as pd

    # Pull DataFrame from XCom
    zori_json = kwargs['ti'].xcom_pull(key='zori_data', task_ids='extract_zori')
    df = pd.read_json(zori_json)

    # Transform and load
    load_zori(df)


def transform_to_staging(**kwargs):
    """Transform raw data into staging layer by executing staging SQL."""
    sql_path = '/opt/airflow/etl/../sql/02_staging.sql'
    if not os.path.exists(sql_path):
        # Fallback to alternative path
        sql_path = '/opt/airflow/dags/../sql/02_staging.sql'

    execute_sql_file(sql_path)


def build_data_marts(**kwargs):
    """Build data marts from staging layer by executing marts SQL."""
    sql_path = '/opt/airflow/etl/../sql/03_marts.sql'
    if not os.path.exists(sql_path):
        # Fallback to alternative path
        sql_path = '/opt/airflow/dags/../sql/03_marts.sql'

    execute_sql_file(sql_path)


def data_quality_check(**kwargs):
    """
    Run data quality checks across all layers.

    Validates:
    - Row counts across layers
    - Null checks on key columns
    - Value range checks (no negative values, etc.)
    """
    from sqlalchemy import create_engine, text
    from etl.load import get_db_connection_string

    conn_string = get_db_connection_string()
    engine = create_engine(conn_string)

    print("=" * 80)
    print("DATA QUALITY CHECKS")
    print("=" * 80)

    with engine.connect() as conn:
        # Check 1: Row counts across all tables
        print("\n1. ROW COUNT CHECKS")
        print("-" * 80)

        tables = ['raw_zhvi', 'raw_zori', 'stg_zhvi', 'stg_zori',
                  'mart_housing_time_series', 'mart_housing_growth']

        row_counts = {}
        for table in tables:
            result = conn.execute(text(f"SELECT COUNT(*) as cnt FROM {table}"))
            count = result.fetchone()[0]
            row_counts[table] = count
            print(f"  {table:30s} : {count:,} rows")

        # Validate row counts
        if row_counts['raw_zhvi'] == 0 or row_counts['raw_zori'] == 0:
            raise ValueError("❌ FAILED: Raw tables are empty!")

        if row_counts['stg_zhvi'] == 0 or row_counts['stg_zori'] == 0:
            raise ValueError("❌ FAILED: Staging tables are empty!")

        if row_counts['mart_housing_time_series'] == 0:
            raise ValueError("❌ FAILED: Mart tables are empty!")

        print("  ✓ All tables have data")

        # Check 2: Null checks on key columns
        print("\n2. NULL CHECKS ON KEY COLUMNS")
        print("-" * 80)

        null_checks = [
            ("raw_zhvi", "state_name"),
            ("raw_zhvi", "date"),
            ("raw_zhvi", "home_value"),
            ("raw_zori", "state_name"),
            ("raw_zori", "date"),
            ("raw_zori", "rent_value"),
            ("stg_zhvi", "state"),
            ("stg_zhvi", "date"),
            ("mart_housing_time_series", "home_value"),
        ]

        for table, column in null_checks:
            result = conn.execute(
                text(f"SELECT COUNT(*) as cnt FROM {table} WHERE {column} IS NULL")
            )
            null_count = result.fetchone()[0]
            if null_count > 0:
                print(f"  ⚠️  {table}.{column}: {null_count} null values")
            else:
                print(f"  ✓ {table}.{column}: no nulls")

        # Check 3: Value range checks (no negative values)
        print("\n3. VALUE RANGE CHECKS")
        print("-" * 80)

        value_checks = [
            ("raw_zhvi", "home_value", 0),
            ("raw_zori", "rent_value", 0),
            ("mart_housing_time_series", "home_value", 0),
            ("mart_housing_time_series", "rent_value", 0),
        ]

        for table, column, min_val in value_checks:
            result = conn.execute(
                text(f"SELECT COUNT(*) as cnt FROM {table} WHERE {column} < {min_val}")
            )
            invalid_count = result.fetchone()[0]
            if invalid_count > 0:
                raise ValueError(
                    f"❌ FAILED: {table}.{column} has {invalid_count} values < {min_val}!"
                )
            else:
                print(f"  ✓ {table}.{column}: all values >= {min_val}")

        # Check 4: Date range checks
        print("\n4. DATE RANGE CHECKS")
        print("-" * 80)

        for table in ['raw_zhvi', 'raw_zori', 'stg_zhvi', 'stg_zori']:
            result = conn.execute(
                text(f"SELECT MIN(date) as min_date, MAX(date) as max_date FROM {table}")
            )
            row = result.fetchone()
            print(f"  {table:30s} : {row[0]} to {row[1]}")

    engine.dispose()

    print("\n" + "=" * 80)
    print("✓ ALL DATA QUALITY CHECKS PASSED")
    print("=" * 80)


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

t_extract_zhvi = PythonOperator(
    task_id="extract_zhvi",
    python_callable=extract_zhvi,
    dag=dag,
)

t_extract_zori = PythonOperator(
    task_id="extract_zori",
    python_callable=extract_zori,
    dag=dag,
)

t_load_zhvi = PythonOperator(
    task_id="load_zhvi_to_raw",
    python_callable=load_zhvi_to_raw,
    dag=dag,
)

t_load_zori = PythonOperator(
    task_id="load_zori_to_raw",
    python_callable=load_zori_to_raw,
    dag=dag,
)

t_transform = PythonOperator(
    task_id="transform_to_staging",
    python_callable=transform_to_staging,
    dag=dag,
)

t_marts = PythonOperator(
    task_id="build_data_marts",
    python_callable=build_data_marts,
    dag=dag,
)

t_quality = PythonOperator(
    task_id="data_quality_check",
    python_callable=data_quality_check,
    dag=dag,
)

# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------

t_extract_zhvi >> t_load_zhvi >> t_transform
t_extract_zori >> t_load_zori >> t_transform
t_transform >> t_marts >> t_quality
