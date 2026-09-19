-- ============================================================
-- Unity Catalog setup for Uber Eats Marketplace Lakehouse
-- Run this in a Databricks SQL notebook (or SQL editor)
-- ============================================================

-- 1. Create the catalog for this project
CREATE CATALOG IF NOT EXISTS uber_eats_lakehouse;

USE CATALOG uber_eats_lakehouse;

-- 2. Create schemas for each medallion layer
CREATE SCHEMA IF NOT EXISTS bronze COMMENT 'Raw, append-only ingested data';
CREATE SCHEMA IF NOT EXISTS silver COMMENT 'Cleaned, conformed data with SCD2 dimensions';
CREATE SCHEMA IF NOT EXISTS gold   COMMENT 'Curated star schema for analytics';

-- 3. Create a Volume in bronze to land the raw synthetic CSV files.
--    This is where you'll upload the raw_data/ folder contents.
CREATE VOLUME IF NOT EXISTS bronze.landing
    COMMENT 'Landing zone for raw source files before Auto Loader ingestion';

-- 4. Sanity check: list what we just created
SHOW SCHEMAS IN uber_eats_lakehouse;
SHOW VOLUMES IN uber_eats_lakehouse.bronze;
