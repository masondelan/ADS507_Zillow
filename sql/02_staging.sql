-- ============================================================================
-- 02_staging.sql
-- Zillow ETL – Staging Tables
-- ============================================================================
-- Purpose:
-- Clean and prepare raw Zillow housing data for analytics.
-- The raw tables are already in long format (one row per state per month)
-- after being unpivoted by the Python ETL pipeline.
--
-- What happens in this layer:
-- - Filter out null values
-- - Clean and standardize data
-- - Apply any business rules or transformations
--
-- Scope / Assumptions:
-- - This version uses STATE-level data only
-- - Raw tables are already in long format from the ETL pipeline
-- - Table design will still work if we switch to metro-level data later
-- ============================================================================

-- Create staging schema if it does not exist
-- CREATE SCHEMA IF NOT EXISTS staging;

-- ============================================================================
-- Staging Table: ZHVI (Home Values)
-- One row per state per month
-- ============================================================================

DROP TABLE IF EXISTS stg_zhvi;

CREATE TABLE stg_zhvi AS
SELECT
    region_id,
    region_name,
    state_name AS state,
    date,
    home_value
FROM raw_zhvi
WHERE state_name IS NOT NULL
  AND home_value IS NOT NULL
  AND date IS NOT NULL;

CREATE INDEX idx_stg_zhvi_state_date ON stg_zhvi(state, date);

-- ============================================================================
-- Staging Table: ZORI (Rent Values)
-- One row per state per month
-- ============================================================================

DROP TABLE IF EXISTS stg_zori;

CREATE TABLE stg_zori AS
SELECT
    region_id,
    region_name,
    state_name AS state,
    date,
    rent_value
FROM raw_zori
WHERE state_name IS NOT NULL
  AND rent_value IS NOT NULL
  AND date IS NOT NULL;

CREATE INDEX idx_stg_zori_state_date ON stg_zori(state, date);
