-- ============================================================================
-- 02_staging.sql
-- Zillow ETL – Staging Tables
-- ============================================================================
-- Purpose:
-- Convert raw Zillow housing data from wide format into a clean, long format
-- that is easier to query, join, and analyze.
--
-- What happens in this layer:
-- - Data is transformed from monthly columns into rows (unpivoted)
-- - Each record represents one state, one month, and one metric
--
-- Scope / Assumptions:
-- - This version uses STATE-level data only
-- - Raw tables contain one row per state with monthly columns
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
    state,
    date,        -- month of observation (from unpivot)
    home_value   -- Zillow Home Value Index
FROM raw_zhvi_all_homes
-- TODO:
-- Unpivot monthly date columns into (date, home_value)
-- This will convert wide time-series data into long format
WHERE state IS NOT NULL;

-- ============================================================================
-- Staging Table: ZORI (Rent Values)
-- One row per state per month
-- ============================================================================

DROP TABLE IF EXISTS stg_zori;

CREATE TABLE stg_zori AS
SELECT
    region_id,
    region_name,
    state,
    date,        -- month of observation (from unpivot)
    rent_value   -- Zillow Observed Rent Index
FROM raw_zori_all_homes
-- TODO:
-- Unpivot monthly date columns into (date, rent_value)
WHERE state IS NOT NULL;

