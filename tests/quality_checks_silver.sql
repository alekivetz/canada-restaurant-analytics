/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the silver layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading silver layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

USE DataWarehouse;
GO

-- ====================================================================
-- Checking 'silver.places'
-- ====================================================================

-- Checking for null or duplicate primary keys
-- Duplicate keys found, other data was consistent across all rows except for lat/lon

SELECT 
    place_id,
    COUNT(*) AS count
FROM bronze.places
GROUP BY place_id
HAVING COUNT(*) > 1 OR place_id IS NULL;

SELECT 
    place_id,
    COUNT(DISTINCT name) as name_variants,
    COUNT(DISTINCT rating) as rating_variants,
    COUNT(DISTINCT user_ratings_total) as user_ratings_total_variants,
    COUNT(DISTINCT price_level) as price_level_variants,
    COUNT(DISTINCT lat) as lat_variants,
    COUNT(DISTINCT lon) as lon_variants,
    COUNT(DISTINCT city) as city_variants
FROM bronze.places
GROUP BY place_id
HAVING COUNT(*) > 1;

-- Checking name column for null values or whitespaces
-- Good data, no null values or whitespaces
SELECT
    name
FROM bronze.places
WHERE name IS NULL OR name != TRIM(name);

-- Checking rating column for null values or negative values
-- Few null values found, keeping
SELECT
    rating
FROM bronze.places
WHERE rating IS NULL OR rating < 0;

-- Checking user_ratings_total column for null values or negative values
-- Few null values found, same as rating column, indicating no reviews yet
SELECT
    rating, 
    user_ratings_total
FROM bronze.places
WHERE user_ratings_total IS NULL OR user_ratings_total < 0;

SELECT *
FROM bronze.places
WHERE 
    (rating IS NULL AND user_ratings_total IS NOT NULL)
    OR
    (rating IS NOT NULL AND user_ratings_total IS NULL);

-- Checking price_level column for null values or negative values
-- Null values found, keeping   
SELECT
    price_level
FROM bronze.places
WHERE price_level IS NULL OR price_level < 0;

-- Checking lat/lon columns for null values
-- Good
SELECT
    lat,
    lon
FROM bronze.places
WHERE lat IS NULL OR lon IS NULL;

-- Checking city column for null values and data consistency
-- Good
SELECT DISTINCT
    city
FROM bronze.places;

-- ====================================================================
-- Checking 'silver.reviews'
-- ====================================================================

SELECT * FROM bronze.reviews;

-- Checking for null foreign keys
-- Good
SELECT 
    place_id
from bronze.reviews
WHERE place_id IS NULL;

-- Checking for nulls or whitespaces in author_name
-- Good
SELECT
    author_name
FROM bronze.reviews
WHERE author_name IS NULL OR author_name != TRIM(author_name);

-- Checking for nulls or negative values in rating
-- Good
SELECT
    rating
FROM bronze.reviews
WHERE rating IS NULL OR rating < 0;

-- Convert review_time to datetime  
SELECT
    review_time,
    DATEADD(SECOND, review_time, '1970-01-01') AS review_time_dt
FROM bronze.reviews;


