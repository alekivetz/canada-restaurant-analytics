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
-- Duplicates are expected due to overlapping coordinate searches during extraction
-- Second query verifies data consistency across duplicate records
-- Result: Duplicates found, data consistent across all fields except price_level
-- Price level variance is minor and will be resolved in silver by keeping
-- the non-null value where available
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

-- Checking price_level for nulls
-- ~200 null values found (~13%) -- expected, not all restaurants have price level set
-- Nulls will be retained in silver

SELECT
    price_level
FROM bronze.google_restaurants
WHERE price_level IS NULL OR price_level < 0;

SELECT DISTINCT price_level FROM bronze.google_restaurants;

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

-- Checking fsa column for null values and data consistency
-- Good
SELECT 
    fsa
FROM bronze.google_restaurants
WHERE fsa IS NULL or fsa != TRIM(fsa);

SELECT DISTINCT fsa FROM bronze.google_restaurants;

-- ====================================================================
-- Checking 'silver.google_reviews'
-- ====================================================================

-- Checking for duplicate primary keys
-- Anything over 5 is a true duplicate as Google returnd 5 reviews per restaurant

SELECT 
    restaurant_id,
    COUNT(*) AS review_count
FROM bronze.google_reviews
GROUP BY restaurant_id
HAVING COUNT(*) > 5
ORDER BY review_count DESC;

-- Checking for duplicate reviews (restaurant_id + author_name combinations)
-- Duplicates are expected due to overlapping coordinate searches during extraction
-- Verifying duplicates have the same review_time (time_diff = 0) to confirm
-- they are true duplicates and not separate reviews from the same author
-- Result: All duplicates have time_diff = 0, safe to deduplicate in silver
SELECT 
    restaurant_id,
    author_name,
    COUNT(*) AS count,
    MIN(review_time) AS earliest,
    MAX(review_time) AS latest,
    MAX(review_time) - MIN(review_time) AS time_diff
FROM bronze.google_reviews
GROUP BY restaurant_id, author_name
HAVING COUNT(*) > 1
ORDER BY time_diff DESC;

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

SELECT DISTINCT rating FROM bronze.google_reviews;

-- Convert review_time to datetime  
SELECT
    review_time,
    DATEADD(SECOND, review_time, '1970-01-01') AS review_time_dt
FROM bronze.google_reviews;

-- ====================================================================
-- Checking 'silver.yelp_restaurants'
-- ====================================================================

SELECT * FROM bronze.yelp_restaurants;

-- Checking for null or duplicate primary keys
-- Result: Duplicates found, all data consistent across duplicate records
-- Two restaurants have null price_level, consistent with Google findings
-- Duplicates will be deduplicated in silver using ROW_NUMBER()
SELECT
    restaurant_id,
    COUNT(*) AS count
FROM bronze.yelp_restaurants
GROUP BY restaurant_id
HAVING COUNT(*) > 1 OR restaurant_id IS NULL;

SELECT
    restaurant_id,
    COUNT(DISTINCT name) as name_variants,
    COUNT(DISTINCT rating) as rating_variants,
    COUNT(DISTINCT categories) as categories_variants,
    COUNT(DISTINCT price_level) as price_level_variants,
    COUNT(DISTINCT city) as city_variants,
    COUNT(DISTINCT lat) as lat_variants,
    COUNT(DISTINCT lon) as lon_variants,
    COUNT(DISTINCT fsa) as fsa_variants
FROM bronze.yelp_restaurants
GROUP BY restaurant_id
HAVING COUNT(*) > 1;

-- Checking name column for null values or whitespaces
-- Good data, no null values or whitespaces
SELECT 
    name
FROM bronze.yelp_restaurants
WHERE name IS NULL OR name != TRIM(name);

-- Checking rating column for null values or negative values
-- Good data, no null values or negative values
SELECT
    rating
FROM bronze.yelp_restaurants
WHERE rating IS NULL OR rating < 0;

-- Checking categories column for null values or whitespaces
-- Good data, no null values or whitespaces
SELECT
    categories
FROM bronze.yelp_restaurants
WHERE categories IS NULL OR categories != TRIM(categories);



