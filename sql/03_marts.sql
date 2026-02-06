-- ============================================================================
-- 03_marts.sql
-- Zillow ETL – Analytics / Mart Tables
-- ============================================================================
-- Purpose:
-- Create analytics-ready tables that power dashboards and reporting.
-- These tables are built from staging data and include derived metrics
-- commonly used in housing market analysis.
--
-- Dashboards and BI tools (Power BI / Tableau) should connect ONLY to
-- the mart tables defined in this file.
-- ============================================================================

-- ============================================================================
-- Mart Table: Housing Time Series
-- One row per state per month
-- Combines home values and rent data into a single view
-- ============================================================================

-- Notes:
-- - Price-to-rent ratio is calculated as:
--   home_value / (rent_value * 12)
-- - This metric is commonly used as a rough indicator of housing affordability

DROP TABLE IF EXISTS mart_housing_time_series;

CREATE TABLE mart_housing_time_series AS
SELECT
    z.region_id,
    z.region_name,
    z.state,
    z.date,
    z.home_value,
    r.rent_value,
    z.home_value / (r.rent_value * 12) AS price_to_rent_ratio
FROM stg_zhvi z
JOIN stg_zori r
  ON z.region_id = r.region_id
 AND z.date = r.date;

-- ============================================================================
-- Mart Table: Housing Growth Metrics
-- Calculates year-over-year (YoY) home value growth
-- ============================================================================

-- Notes:
-- - YoY growth compares each month to the same month in the prior year
-- - Window functions are used to avoid self-joins and improve readability

DROP TABLE IF EXISTS mart_housing_growth;

CREATE TABLE mart_housing_growth AS
SELECT
    region_id,
    region_name,
    state,
    date,
    home_value,
    LAG(home_value, 12) OVER (
        PARTITION BY region_id
        ORDER BY date
    ) AS home_value_last_year,
    (home_value - LAG(home_value, 12) OVER (
        PARTITION BY region_id
        ORDER BY date
    )) / LAG(home_value, 12) OVER (
        PARTITION BY region_id
        ORDER BY date
    ) AS home_value_yoy
FROM stg_zhvi;

