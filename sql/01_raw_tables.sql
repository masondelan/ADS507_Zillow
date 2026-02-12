-- ============================================================================
-- Zillow Housing Data - Raw Tables
-- ============================================================================
-- This script creates the raw schema and tables for storing Zillow data
-- as extracted from the source without transformations.
-- ============================================================================

-- Create raw schema
-- CREATE SCHEMA IF NOT EXISTS raw;

-- ============================================================================
-- ZHVI (Zillow Home Value Index) Raw Table
-- ============================================================================
-- This table stores home value data in long format (one row per state per month)
-- after unpivoting from the wide-format CSV source

DROP TABLE IF EXISTS raw_zhvi;

CREATE TABLE raw_zhvi (
    region_id INT,
    size_rank INT,
    region_name VARCHAR(255),
    region_type VARCHAR(50),
    state_name VARCHAR(2),
    date DATE NOT NULL,
    home_value NUMERIC(12, 2),
    PRIMARY KEY (region_id, date)
);

CREATE INDEX idx_raw_zhvi_state_date ON raw_zhvi(state_name, date);
CREATE INDEX idx_raw_zhvi_date ON raw_zhvi(date);

-- ============================================================================
-- ZORI (Zillow Observed Rent Index) Raw Table
-- ============================================================================
-- This table stores rental value data in long format (one row per state per month)
-- after unpivoting from the wide-format CSV source

DROP TABLE IF EXISTS raw_zori;

CREATE TABLE raw_zori (
    region_id INT,
    size_rank INT,
    region_name VARCHAR(255),
    region_type VARCHAR(50),
    state_name VARCHAR(2),
    date DATE NOT NULL,
    rent_value NUMERIC(12, 2),
    PRIMARY KEY (region_id, date)
);

CREATE INDEX idx_raw_zori_state_date ON raw_zori(state_name, date);
CREATE INDEX idx_raw_zori_date ON raw_zori(date);
