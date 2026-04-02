/*
===============================================================================
Quality Checks: Bronze and Silver Layer
===============================================================================
Script Purpose:
    This script performs quality checks against the bronze layer before loading
    silver, and against the silver layer after loading to verify transformations
    were applied correctly. Checks include:
    - Null or duplicate primary keys
    - Unwanted spaces in string fields
    - Data standardization and consistency
    - Cross-source validation

Usage Notes:
    - Run bronze checks before loading the silver layer
    - Run silver checks after loading the silver layer
    - Investigate and resolve any discrepancies found during the checks
    - All results are documented inline with findings
===============================================================================
*/

USE DataWarehouse;
GO


-- ====================================================================
-- BRONZE CHECKS
-- ====================================================================


-- ====================================================================
-- Checking 'bronze.google_restaurants'
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
    COUNT(DISTINCT name) AS name_variants,
    COUNT(DISTINCT rating) AS rating_variants,
    COUNT(DISTINCT user_ratings_total) AS user_ratings_total_variants,
    COUNT(DISTINCT price_level) AS price_level_variants,
    COUNT(DISTINCT lat) AS lat_variants,
    COUNT(DISTINCT lon) AS lon_variants,
    COUNT(DISTINCT city) AS city_variants
FROM bronze.google_restaurants
GROUP BY restaurant_id
HAVING COUNT(*) > 1;

-- Checking name column for null values or whitespaces
-- Result: Good data, no null values or whitespaces
SELECT
    name
FROM bronze.google_restaurants
WHERE name IS NULL OR name != TRIM(name);

-- Checking rating column for null values or negative values
-- Result: Few null values found, keeping - indicates restaurants with no reviews yet
SELECT
    rating
FROM bronze.google_restaurants
WHERE rating IS NULL OR rating < 0;

-- Checking user_ratings_total column for null values or negative values
-- Result: Few null values found, consistent with rating nulls - indicates no reviews yet
SELECT
    rating,
    user_ratings_total
FROM bronze.google_restaurants
WHERE user_ratings_total IS NULL OR user_ratings_total < 0;

-- Checking for mismatched nulls between rating and user_ratings_total
-- Result: Good - nulls are consistent across both columns
SELECT *
FROM bronze.google_restaurants
WHERE
    (rating IS NULL AND user_ratings_total IS NOT NULL)
    OR
    (rating IS NOT NULL AND user_ratings_total IS NULL);

-- Checking price_level for nulls
-- Result: ~200 null values found (~13%) - expected, not all restaurants have price level set
-- Nulls will be standardized to 'N/A' in silver
SELECT DISTINCT
    price_level
FROM bronze.google_restaurants;

SELECT
    price_level
FROM bronze.google_restaurants
WHERE price_level IS NULL OR price_level < 0;

-- Checking lat/lon columns for null values
-- Result: Good, no null values
SELECT
    lat,
    lon
FROM bronze.google_restaurants
WHERE lat IS NULL OR lon IS NULL;

-- Checking city column for null values and data consistency
-- Result: Good, all cities match predefined target cities
SELECT DISTINCT
    city
FROM bronze.google_restaurants;

-- Checking fsa column for null values and data consistency
-- Result: Good, no null values or whitespaces
SELECT
    fsa
FROM bronze.google_restaurants
WHERE fsa IS NULL OR fsa != TRIM(fsa);

SELECT DISTINCT fsa
FROM bronze.google_restaurants;

-- Checking phone_number column for null values and standardization
-- Phone numbers will be cleaned using dbo.strip_non_numeric()
-- Valid Canadian phone numbers should be 11 digits (1XXXXXXXXXX)
-- Result: 26 null values found, 1 number with extension - handled by function
SELECT
    phone_number
FROM bronze.google_restaurants
WHERE phone_number IS NULL OR phone_number != TRIM(phone_number);

SELECT
    restaurant_id,
    name,
    phone_number,
    dbo.strip_non_numeric(phone_number) AS phone_cleaned,
    LEN(dbo.strip_non_numeric(phone_number)) AS phone_length
FROM bronze.google_restaurants
WHERE phone_number IS NOT NULL
    AND LEN(dbo.strip_non_numeric(phone_number)) != 11;


-- ====================================================================
-- Checking 'bronze.yelp_restaurants'
-- ====================================================================

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
    COUNT(DISTINCT name) AS name_variants,
    COUNT(DISTINCT rating) AS rating_variants,
    COUNT(DISTINCT categories) AS categories_variants,
    COUNT(DISTINCT price_level) AS price_level_variants,
    COUNT(DISTINCT city) AS city_variants,
    COUNT(DISTINCT lat) AS lat_variants,
    COUNT(DISTINCT lon) AS lon_variants,
    COUNT(DISTINCT fsa) AS fsa_variants
FROM bronze.yelp_restaurants
GROUP BY restaurant_id
HAVING COUNT(*) > 1;

-- Checking name column for null values or whitespaces
-- Result: Good data, no null values or whitespaces
SELECT
    name
FROM bronze.yelp_restaurants
WHERE name IS NULL OR name != TRIM(name);

-- Checking rating column for null values or negative values
-- Result: Good data, no null values or negative values
SELECT
    rating
FROM bronze.yelp_restaurants
WHERE rating IS NULL OR rating < 0;

-- Checking categories column for null values or whitespaces
-- Result: Good data, no null values or whitespaces
SELECT
    categories
FROM bronze.yelp_restaurants
WHERE categories IS NULL OR categories != TRIM(categories);

-- Checking price_level for null values
-- Result: Some nulls found - will be standardized to 'N/A' in silver
-- Yelp uses string format ($, $$, $$$, $$$$) unlike Google's integer format
SELECT DISTINCT
    price_level
FROM bronze.yelp_restaurants;

-- Checking city column for null values and data consistency
-- Result: Yelp returns neighbourhood/suburb names based on restaurant address
-- rather than the broader city used during extraction
-- These will be standardized in silver using a CASE statement
-- Ottawa excluded as it falls outside target cities (likely a border case from coordinate search)
SELECT
    city
FROM bronze.yelp_restaurants
WHERE city IS NULL OR city != TRIM(city);

SELECT
    city,
    COUNT(*) AS count
FROM bronze.yelp_restaurants
GROUP BY city
HAVING city NOT IN ('Edmonton', 'Calgary', 'Toronto', 'Vancouver', 'Montreal')
ORDER BY count DESC;

-- Checking lat/lon columns for null values
-- Result: 2 null values found - will be retained as NULL in silver
SELECT
    lat,
    lon
FROM bronze.yelp_restaurants
WHERE lat IS NULL OR lon IS NULL;

-- Checking fsa column for null values and data consistency
-- Result: 2 null values found consistent with lat/lon nulls - no FSA lookup possible
SELECT
    fsa
FROM bronze.yelp_restaurants
WHERE fsa IS NULL OR fsa != TRIM(fsa);

-- Checking phone_number column for null values and standardization
-- Phone numbers will be cleaned using dbo.strip_non_numeric()
-- Valid Canadian phone numbers should be 11 digits (1XXXXXXXXXX)
-- Result: No nulls found
SELECT
    phone_number
FROM bronze.yelp_restaurants
WHERE phone_number IS NULL OR phone_number != TRIM(phone_number);

SELECT
    restaurant_id,
    name,
    phone_number,
    dbo.strip_non_numeric(phone_number) AS phone_cleaned,
    LEN(dbo.strip_non_numeric(phone_number)) AS phone_length
FROM bronze.yelp_restaurants
WHERE phone_number IS NOT NULL
    AND LEN(dbo.strip_non_numeric(phone_number)) != 11;


-- ====================================================================
-- Checking 'bronze.google_reviews'
-- ====================================================================

-- Checking for duplicate reviews
-- Anything over 5 is a true duplicate as Google returns 5 reviews per restaurant
-- Duplicates occur because the same restaurant can appear in multiple coordinate searches
SELECT
    restaurant_id,
    COUNT(*) AS review_count
FROM bronze.google_reviews
GROUP BY restaurant_id
HAVING COUNT(*) > 5
ORDER BY review_count DESC;

-- Checking for duplicate reviews by restaurant_id + author_name
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
-- Result: Good, no null restaurant_ids
SELECT
    restaurant_id
FROM bronze.google_reviews
WHERE restaurant_id IS NULL;

-- Checking for nulls or whitespaces in author_name
-- Result: Good, no null values or whitespaces
SELECT
    author_name
FROM bronze.google_reviews
WHERE author_name IS NULL OR author_name != TRIM(author_name);

-- Checking for nulls or negative values in rating
-- Result: Good, no null values or negative values
SELECT
    rating
FROM bronze.google_reviews
WHERE rating IS NULL OR rating < 0;

SELECT DISTINCT rating
FROM bronze.google_reviews;

-- Verifying review_time Unix timestamp conversion to datetime
-- review_time is stored as Unix timestamp (seconds since 1970-01-01)
-- Will be converted to DATETIME in silver
SELECT
    review_time,
    DATEADD(SECOND, review_time, CAST('1970-01-01' AS DATETIME)) AS review_time_dt
FROM bronze.google_reviews;


-- ====================================================================
-- Checking 'bronze.census_2021'
-- ====================================================================

-- Checking fsa for nulls or whitespaces
-- Result: Good, no null values or whitespaces
SELECT
    fsa
FROM bronze.census_2021
WHERE fsa IS NULL OR fsa != TRIM(fsa);

-- Checking variable for nulls or whitespaces and data consistency
-- Result: Good, only 3 expected variables present
SELECT
    variable
FROM bronze.census_2021
WHERE variable IS NULL OR variable != TRIM(variable);

SELECT DISTINCT variable
FROM bronze.census_2021;

-- Checking value for nulls or negative values
-- Result: Some nulls found - expected for suppressed StatCan data
-- Negative values would be unexpected for population and age
-- Some negative values possible for median income in edge cases
SELECT
    value
FROM bronze.census_2021
WHERE value IS NULL OR value < 0;


-- ====================================================================
-- SILVER CHECKS
-- ====================================================================


-- ====================================================================
-- Checking 'silver.google_restaurants'
-- ====================================================================

-- Checking deduplication - no duplicate restaurant_ids should remain
-- Result: Good, no duplicates
SELECT
    restaurant_id,
    COUNT(*) AS count
FROM silver.google_restaurants
GROUP BY restaurant_id
HAVING COUNT(*) > 1 OR restaurant_id IS NULL;

-- Checking price_level standardization
-- Result: Should only contain $, $$, $$$, $$$$ or N/A
SELECT DISTINCT
    price_level
FROM silver.google_restaurants;

-- Checking phone_number standardization
-- Result: All phone numbers should be 11 digits or NULL
SELECT
    restaurant_id,
    name,
    phone_number
FROM silver.google_restaurants
WHERE phone_number IS NOT NULL
    AND LEN(phone_number) != 11;

-- Checking city consistency
-- Result: Should only contain target cities
SELECT DISTINCT
    city
FROM silver.google_restaurants;


-- ====================================================================
-- Checking 'silver.yelp_restaurants'
-- ====================================================================

-- Checking deduplication - no duplicate restaurant_ids should remain
-- Result: Good, no duplicates
SELECT
    restaurant_id,
    COUNT(*) AS count
FROM silver.yelp_restaurants
GROUP BY restaurant_id
HAVING COUNT(*) > 1 OR restaurant_id IS NULL;

-- Checking price_level standardization
-- Result: Should only contain $, $$, $$$, $$$$ or NULL
SELECT DISTINCT
    price_level
FROM silver.yelp_restaurants;

-- Checking city standardization
-- Result: Should only contain target cities or NULL (Ottawa)
SELECT DISTINCT
    city
FROM silver.yelp_restaurants;

-- Checking phone_number standardization
-- Result: All phone numbers should be 11 digits or NULL
SELECT
    restaurant_id,
    name,
    phone_number
FROM silver.yelp_restaurants
WHERE phone_number IS NOT NULL
    AND LEN(phone_number) != 11;


-- ====================================================================
-- Checking 'silver.restaurants'
-- ====================================================================

-- Checking source column consistency
-- Result: Should only contain 'both', 'google', 'yelp'
SELECT DISTINCT
    source
FROM silver.restaurants;

-- Checking source distribution
-- Result: Shows match rate between Google and Yelp
SELECT
    source,
    COUNT(*) AS count
FROM silver.restaurants
GROUP BY source;

-- Checking for null restaurant_id and yelp_id by source
-- Result: Google-only records should have NULL yelp_id
--         Yelp-only records should have NULL restaurant_id
--         Both records should have neither NULL
SELECT
    source,
    COUNT(*) AS total,
    SUM(CASE WHEN restaurant_id IS NULL THEN 1 ELSE 0 END) AS null_google_id,
    SUM(CASE WHEN yelp_id IS NULL THEN 1 ELSE 0 END) AS null_yelp_id
FROM silver.restaurants
GROUP BY source;

-- Checking city consistency
-- Result: Should only contain target cities
SELECT DISTINCT
    city
FROM silver.restaurants
ORDER BY city;

-- Checking for null lat/lon
-- Result: Should be minimal
SELECT
    restaurant_id,
    yelp_id,
    name,
    city,
    lat,
    lon
FROM silver.restaurants
WHERE lat IS NULL OR lon IS NULL;


-- ====================================================================
-- Checking 'silver.google_reviews'
-- ====================================================================

-- Checking deduplication - no restaurant/author duplicates should remain
-- Result: Good, no duplicates
SELECT
    restaurant_id,
    author_name,
    COUNT(*) AS count
FROM silver.google_reviews
GROUP BY restaurant_id, author_name
HAVING COUNT(*) > 1;

-- Checking review_time conversion
-- Result: Should be valid datetime values
SELECT
    review_time
FROM silver.google_reviews
WHERE review_time IS NULL OR review_time < '2000-01-01';


-- ====================================================================
-- Checking 'silver.census_2021'
-- ====================================================================

-- Checking fsa for nulls
-- Result: Good, no null values
SELECT
    fsa
FROM silver.census_2021
WHERE fsa IS NULL;

-- Checking variable consistency
-- Result: Should only contain population, average_age, median_income
SELECT DISTINCT
    variable
FROM silver.census_2021;

-- Checking value for nulls or unexpected negatives
SELECT
    variable,
    COUNT(*) AS total,
    SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) AS null_count,
    SUM(CASE WHEN value < 0 THEN 1 ELSE 0 END) AS negative_count,
    MIN(value) AS min_value,
    MAX(value) AS max_value
FROM silver.census_2021
GROUP BY variable;