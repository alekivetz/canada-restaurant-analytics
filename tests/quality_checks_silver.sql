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

Usage Notes:
    - Run these checks after data loading silver layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

USE DataWarehouse;
GO

-- ====================================================================
-- Checking 'silver.google_restaurants'
-- ====================================================================

-- Checking for null or duplicate primary keys
-- Duplicate keys found, other data was consistent across all rows except for lat/lon

SELECT 
    restaurant_id,
    COUNT(*) AS count
FROM bronze.google_restaurants
GROUP BY restaurant_id
HAVING COUNT(*) > 1 OR restaurant_id IS NULL;

SELECT 
    restaurant_id,
    COUNT(DISTINCT name) as name_variants,
    COUNT(DISTINCT rating) as rating_variants,
    COUNT(DISTINCT user_ratings_total) as user_ratings_total_variants,
    COUNT(DISTINCT price_level) as price_level_variants,
    COUNT(DISTINCT lat) as lat_variants,
    COUNT(DISTINCT lon) as lon_variants,
    COUNT(DISTINCT city) as city_variants
FROM bronze.google_restaurants
GROUP BY restaurant_id
HAVING COUNT(*) > 1;

-- Checking name column for null values or whitespaces
-- Good data, no null values or whitespaces
SELECT
    name
FROM bronze.google_restaurants
WHERE name IS NULL OR name != TRIM(name);

-- Checking rating column for null values or negative values
-- Few null values found, keeping
SELECT
    rating
FROM bronze.google_restaurants
WHERE rating IS NULL OR rating < 0;

-- Checking user_ratings_total column for null values or negative values
-- Few null values found, same as rating column, indicating no reviews yet
SELECT
    rating, 
    user_ratings_total
FROM bronze.google_restaurants
WHERE user_ratings_total IS NULL OR user_ratings_total < 0;

SELECT *
FROM bronze.google_restaurants
WHERE 
    (rating IS NULL AND user_ratings_total IS NOT NULL)
    OR
    (rating IS NOT NULL AND user_ratings_total IS NULL);

-- Checking price_level column for null values or negative values
-- Null values found, keeping   
SELECT
    price_level
FROM bronze.google_restaurants
WHERE price_level IS NULL OR price_level < 0;

-- Checking lat/lon columns for null values
-- Good
SELECT
    lat,
    lon
FROM bronze.google_restaurants
WHERE lat IS NULL OR lon IS NULL;

-- Checking city column for null values and data consistency
-- Good
SELECT DISTINCT
    city
FROM bronze.google_restaurants;

-- ====================================================================
-- Checking 'silver.google_reviews'
-- ====================================================================

-- Checking for null foreign keys
-- Good
SELECT 
    restaurant_id
from bronze.google_reviews
WHERE restaurant_id IS NULL;

-- Checking for nulls or whitespaces in author_name
-- Good
SELECT
    author_name
FROM bronze.google_reviews
WHERE author_name IS NULL OR author_name != TRIM(author_name);

-- Checking for nulls or negative values in rating
-- Good
SELECT
    rating
FROM bronze.google_reviews
WHERE rating IS NULL OR rating < 0;

-- Convert review_time to datetime  
SELECT
    review_time,
    DATEADD(SECOND, review_time, '1970-01-01') AS review_time_dt
FROM bronze.google_reviews;

-- ====================================================================
-- Checking 'silver.google_categories'
-- ====================================================================

SELECT * FROM bronze.google_categories;

--- Checking for null foreign keys
-- Good
SELECT restaurant_id
FROM bronze.google_categories
WHERE restaurant_id IS NULL;

-- Data null or whitespaces in category column
-- Good
SELECT category
FROM bronze.google_categories
WHERE category IS NULL OR category != TRIM(category);

-- Data consistency in category column
SELECT DISTINCT category
FROM bronze.google_categories;

