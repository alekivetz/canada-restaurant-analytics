/*
===============================================================================
Quality Checks: Gold Layer
===============================================================================
Script Purpose:
    This script performs quality checks against the gold layer after loading
    to verify the star schema is correctly built. Checks include:
    - Null or duplicate primary and surrogate keys
    - Orphaned foreign keys
    - Data standardization and consistency
    - Valid ranges for metrics

Usage Notes:
    - Run after executing ddl_gold.sql
    - Investigate and resolve any discrepancies found during the checks
    - All results are documented inline with findings
===============================================================================
*/

USE DataWarehouse;
GO

-- =============================================================================
-- Checking Dimension Table: gold.dim_restaurant
-- =============================================================================

--- Check for duplicates/nulls in restaurant_key and both google_id and yelp_id
-- Good
SELECT
    restaurant_key, 
    COUNT(*) AS count_restaurant_key
FROM gold.dim_restaurant
GROUP BY restaurant_key
HAVING COUNT(*) > 1 OR restaurant_key IS NULL;

SELECT 
    google_id,
    COUNT(*) AS count_google_id
FROM gold.dim_restaurant
WHERE google_id IS NOT NULL
GROUP BY google_id
HAVING COUNT(*) > 1;

SELECT
    yelp_id,
    COUNT(*) AS count_yelp_id
FROM gold.dim_restaurant
WHERE yelp_id IS NOT NULL
GROUP BY yelp_id
HAVING COUNT(*) > 1;

--- Check for nulls or whitespace in name
-- Good
SELECT
    name
FROM gold.dim_restaurant
WHERE name IS NULL OR name != TRIM(name);

-- Check that source column is either 'google', 'yelp', or 'both'
-- Good
SELECT DISTINCT 
    source
FROM gold.dim_restaurant;

-- Check for null google_ids for non-yelp records
-- Good
SELECT
    google_id
FROM gold.dim_restaurant
WHERE source IN ('google', 'both')
AND google_id IS NULL;

--- Check for null yelp_ids for non-google records
-- Good
SELECT
    yelp_id
FROM gold.dim_restaurant
WHERE source IN ('yelp', 'both')
AND yelp_id IS NULL;

-- =============================================================================
-- Checking Dimension Table: gold.dim_location
-- =============================================================================

--- Check for duplicates/nulls in location_key and fsa
-- Good
SELECT
    location_key,
    COUNT(*) AS count_location_key
FROM gold.dim_location
GROUP BY location_key
HAVING COUNT(*) > 1 OR location_key IS NULL;

-- Check for nulls or whitespace in city
-- Good
SELECT
    city
FROM gold.dim_location
WHERE city IS NULL OR city != TRIM(city);

-- Check for data consistency in city - should only be specified cities
-- Good
SELECT DISTINCT 
    city
FROM gold.dim_location;

-- Check for nulls or whitespace in fsa
-- Good
SELECT
    fsa
FROM gold.dim_location
WHERE fsa IS NULL OR fsa != TRIM(fsa);

-- Check for nulls/negatives in population, average_age, and median_income
-- Result: One null found in median_income - expected, StatCan suppresses income
-- data for small populations to protect privacy. Population and average_age clean.
SELECT
    population,
    average_age,
    median_income
FROM gold.dim_location
WHERE population IS NULL OR average_age IS NULL OR median_income IS NULL
OR population < 0 OR average_age < 0 OR median_income < 0;

-- =============================================================================
-- Checking Dimension Table: gold.dim_category
-- =============================================================================

-- Check for duplicates/nulls in category_key
-- Good
SELECT
    category_key,
    COUNT(*) AS count_category_key
FROM gold.dim_category
GROUP BY category_key
HAVING COUNT(*) > 1 OR category_key IS NULL;

-- Check for nulls in restaurant_key
-- Good
SELECT
    restaurant_key
FROM gold.dim_category
WHERE restaurant_key IS NULL;

-- Check for orphaned restaurant keys
-- Good
SELECT
    restaurant_key
FROM gold.dim_category
WHERE restaurant_key NOT IN (SELECT restaurant_key FROM gold.dim_restaurant);

-- Check for nulls or whitespace in category
-- Good
SELECT
    category
FROM gold.dim_category
WHERE category IS NULL OR category != TRIM(category);

-- =============================================================================
-- Checking Fact Table: gold.fact_restaurants
-- =============================================================================

-- Check for duplicates/nulls in restaurant_key
-- Good
SELECT
    restaurant_key,
    COUNT(*) AS count_restaurant_key
FROM gold.fact_restaurants
GROUP BY restaurant_key
HAVING COUNT(*) > 1 OR restaurant_key IS NULL;

-- Check for orphaned restaurant keys
-- Good
SELECT
    restaurant_key
FROM gold.fact_restaurants
WHERE restaurant_key NOT IN (SELECT restaurant_key FROM gold.dim_restaurant);

-- Check for nulls in location_key
-- Result: 2 null location keys found - expected, restaurants with no FSA
-- due to null coordinates during extraction
SELECT
    location_key
FROM gold.fact_restaurants
WHERE location_key IS NULL;

-- Check for orphaned location keys
-- Good
SELECT
    location_key
FROM gold.fact_restaurants
WHERE location_key NOT IN (SELECT location_key FROM gold.dim_location);

-- Check ratings between 0 and 5 or null
-- Good
SELECT
    google_rating,
    yelp_rating
FROM gold.fact_restaurants
WHERE (google_rating < 0 OR google_rating > 5)
OR (yelp_rating < 0 OR yelp_rating > 5);

-- Check price levels are valid
-- Good
SELECT DISTINCT
    google_price_level,
    yelp_price_level
FROM gold.fact_restaurants
WHERE google_price_level NOT IN ('$', '$$', '$$$', '$$$$') AND google_price_level IS NOT NULL
OR yelp_price_level NOT IN ('$', '$$', '$$$', '$$$$', NULL) AND yelp_price_level IS NOT NULL;

-- =============================================================================
-- Checking Fact Table: gold.fact_reviews
-- =============================================================================

-- Check for duplicates/nulls in review_key
-- Good
SELECT
    review_key,
    COUNT(*) AS count_review_key
FROM gold.fact_reviews
GROUP BY review_key
HAVING COUNT(*) > 1 OR review_key IS NULL;

-- Check for nulls in restaurant_key
-- Good
SELECT
    restaurant_key
FROM gold.fact_reviews
WHERE restaurant_key IS NULL;

-- Check for orphaned restaurant keys
-- Good
SELECT
    restaurant_key
FROM gold.fact_reviews
WHERE restaurant_key NOT IN (SELECT restaurant_key FROM gold.dim_restaurant);

-- Check for nulls/whitespace in author_name
-- Good
SELECT
    author_name
FROM gold.fact_reviews
WHERE author_name IS NULL OR author_name != TRIM(author_name);

-- Check for nulls/whitespace in text
-- Good
SELECT
    text
FROM gold.fact_reviews
WHERE text IS NULL OR text != TRIM(text);

-- Check for nulls and data consistency in rating
-- Good
SELECT
    rating
FROM gold.fact_reviews
WHERE rating < 0 OR rating > 5;

-- Check for nulls in review_time
-- Good
SELECT
    review_time
FROM gold.fact_reviews
WHERE review_time IS NULL;


