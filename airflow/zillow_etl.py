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

from airflow import DAG
from airflow.operators.python import PythonOperator


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
# Task callables – stubs until extract.py / load.py are implemented
# ---------------------------------------------------------------------------


def extract_zhvi(**kwargs):
    print("Extracting ZHVI data from Zillow Research...")


def extract_zori(**kwargs):
    print("Extracting ZORI data from Zillow Research...")


def load_zhvi_to_raw(**kwargs):
    print("Loading ZHVI data into raw.raw_zhvi...")


def load_zori_to_raw(**kwargs):
    print("Loading ZORI data into raw.raw_zori...")


def transform_to_staging(**kwargs):
    print("Transforming raw data into staging layer...")


def build_data_marts(**kwargs):
    print("Building data marts from staging layer...")


def data_quality_check(**kwargs):
    print("Running data quality checks...")


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
