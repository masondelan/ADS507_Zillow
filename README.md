# Zillow Housing Data ETL Pipeline

An automated data pipeline for extracting, transforming, and loading Zillow housing market data using Apache Airflow, PostgreSQL, and Docker.

## Project Structure

```
├── docker-compose.yml          # Docker services configuration
├── airflow/                    # Airflow DAGs and configs
│   └── dags/
│       └── zillow_etl.py       # Main ETL pipeline DAG
├── etl/                        # ETL Python modules
│   ├── extract.py              # Data extraction from Zillow
│   ├── load.py                 # Data loading to PostgreSQL
│   └── requirements.txt        # Python dependencies
├── sql/                        # Database SQL scripts
│   ├── 01_raw_tables.sql       # Raw layer table definitions
│   ├── 02_staging.sql          # Staging transformations
│   └── 03_marts.sql            # Analytics data marts
├── docker/                     # Docker build files
│   └── Dockerfile.airflow      # Custom Airflow image
├── docs/                       # Documentation
│   ├── architecture.png        # System architecture diagram
│   └── design_doc.pdf          # Detailed design document
└── README.md                   # This file
```

## Zillow Research Data

This pipeline extracts housing market data from [Zillow Research](https://www.zillow.com/research/data/), which provides free, publicly available housing data.

### Datasets Used

#### ZHVI (Zillow Home Value Index)

The Zillow Home Value Index (ZHVI) is a smoothed, seasonally adjusted measure of the typical home value and market changes across a given region and housing type. This pipeline uses the **All Homes (35th–65th percentile)** dataset at the **state level**.

- Typical U.S. home value (2025): ~$368,000

For methodology details, see [Zillow's ZHVI documentation](https://www.zillow.com/research/methodology-neural-zhvi-32128/).

#### ZORI (Zillow Observed Rent Index)

The Zillow Observed Rent Index (ZORI) is a smoothed measure of the typical observed market rate rent across a given region. This pipeline uses the **All Homes (40th–60th percentile)** dataset at the **state level**.

- National rent index (2025): ~$2,049/month

For methodology details, see [Zillow's ZORI documentation](https://www.zillow.com/research/methodology-zori-repeat-rent-27092/).

### Data Characteristics

- **Update Frequency:** Monthly
- **Historical Data:** Available from 2000 onwards
- **Source Format:** CSV files in wide format (dates as columns)
- **Data Quality:** Smoothed and seasonally adjusted by Zillow

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Git

### Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/masondelan/ADS507_Zillow.git
   cd ADS507_Zillow
   ```

2. Start the services:

   ```bash
   docker compose up -d
   ```

   This builds a custom Airflow image and launches five containers: the Airflow metadata database, the pipeline database, an initialization container, the Airflow webserver, and the Airflow scheduler.

3. Access the Airflow UI:

   - URL: http://localhost:8080
   - Username: `admin`
   - Password: `admin`

4. Enable and trigger the `zillow_etl_pipeline` DAG.

### Services

| Service            | Port | Description                          |
|--------------------|------|--------------------------------------|
| Airflow Webserver  | 8080 | Airflow web UI                       |
| Airflow Scheduler  | —    | Executes DAG tasks on schedule       |
| Pipeline DB        | 5434 | PostgreSQL database for housing data |
| Airflow DB         | 5432 | PostgreSQL metadata database         |

## Data Architecture

The pipeline follows a three-layer medallion architecture. All housing data is stored in the **pipeline-db** PostgreSQL instance.

### Layer 1: Raw (Bronze)

Raw data as extracted from Zillow, stored in long format after unpivoting from the original wide CSV structure.

- `raw_zhvi` — Home value index data (one row per state per month)
- `raw_zori` — Rental index data (one row per state per month)

### Layer 2: Staging (Silver)

Cleaned and filtered data ready for analytics joins.

- `stg_zhvi` — Cleaned home values with null states removed
- `stg_zori` — Cleaned rental values with null states removed

### Layer 3: Marts (Gold)

Analytics-ready tables for dashboards and reporting.

- `mart_housing_time_series` — Combined home values and rent data per state per month, with price-to-rent ratio
- `mart_housing_growth` — Year-over-year home value growth calculated using window functions

## ETL Pipeline

The Airflow DAG `zillow_etl_pipeline` orchestrates the following tasks:

```
[extract_zhvi] ──► [load_zhvi_to_raw] ──┐
                                        ├──► [transform_to_staging] ──► [build_data_marts] ──► [data_quality_check]
[extract_zori] ──► [load_zori_to_raw] ──┘
```

### Task Descriptions

| Task                   | Description                                                        |
|------------------------|--------------------------------------------------------------------|
| `extract_zhvi`         | Downloads ZHVI CSV data from Zillow Research                       |
| `extract_zori`         | Downloads ZORI CSV data from Zillow Research                       |
| `load_zhvi_to_raw`     | Unpivots and loads ZHVI data into the raw layer                    |
| `load_zori_to_raw`     | Unpivots and loads ZORI data into the raw layer                    |
| `transform_to_staging` | Cleans and filters raw data into staging tables                    |
| `build_data_marts`     | Builds analytics tables with derived metrics (YoY growth, ratios)  |
| `data_quality_check`   | Validates row counts, null checks on key columns, value ranges (no negatives), and date ranges across all layers |

### Schedule

The pipeline runs monthly (`@monthly`) to align with Zillow's data update frequency. Catchup is disabled.

## Docker Configuration

The project uses a custom Airflow image built from `docker/Dockerfile.airflow` on top of Airflow 2.8.4 with PostgreSQL 15 for both the Airflow metadata database and the pipeline database. Environment variables for the pipeline database connection are passed to all Airflow containers via `PIPE_DB_*` variables. The ETL modules in `etl/` are mounted into the Airflow containers and added to `PYTHONPATH` so DAG tasks can import them directly.

## Development

### Local Development Setup

1. Create a virtual environment:

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. Install dependencies:

   ```bash
   pip install -r etl/requirements.txt
   ```

3. Run extraction locally:

   ```bash
   python etl/extract.py
   ```

## Data Sources

- **Zillow Research:** https://www.zillow.com/research/data/
- **ZHVI Methodology:** https://www.zillow.com/research/methodology-neural-zhvi-32128/
- **ZORI Methodology:** https://www.zillow.com/research/methodology-zori-repeat-rent-27092/

## Team

- Mason Delan
- Titus Sun
- Sushma Kafle

## License

This project is for educational purposes as part of ADS 507 at the University of San Diego. Zillow data is subject to [Zillow's terms of use](https://www.zillow.com/z/corp/terms/).
