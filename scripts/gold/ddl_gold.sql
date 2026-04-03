/*
===============================================================================
DDL Script: Create Gold Tables
===============================================================================
Script Purpose:
    This script creates tables for the gold layer of the DataWarehouse,
    implementing a star schema for analytical reporting and business intelligence.
    
    Tables are built by transforming and combining data from the silver layer,
    producing clean, enriched, business-ready datasets.

    The gold layer consists of:
        - Dimension tables: dim_restaurant, dim_location, dim_category
        - Fact tables: fact_restaurants, fact_reviews

Usage:
    - Run this script to create or recreate all gold layer tables
    - Requires silver layer tables to be populated before running
    - Tables can be queried directly for analytics and reporting

WARNING:
    Running this script will drop and recreate all gold tables.
    All existing data in the gold layer will be permanently deleted.
===============================================================================
*/

USE DataWarehouse;
GO

-- =============================================================================
-- Create Dimension Table: gold.dim_restaurant
-- =============================================================================

If OBJECT_ID('gold.dim_restaurant', 'U') IS NOT NULL
    DROP TABLE gold.dim_restaurant;
GO

SELECT
    ROW_NUMBER() OVER (ORDER BY name) AS restaurant_key, -- Surrogate key
    google_id,
    yelp_id,
    name,
    lat,
    lon,
    source
INTO gold.dim_restaurant
FROM silver.restaurants;
GO

-- =============================================================================
-- Create Dimension Table: gold.dim_location
-- =============================================================================

IF OBJECT_ID('gold.dim_location', 'U') IS NOT NULL
    DROP TABLE gold.dim_location;
GO

SELECT
    ROW_NUMBER() OVER (ORDER BY fsa) AS location_key, -- Surrogate key
    city,
    fsa,
    population,
    average_age,
    median_income
INTO gold.dim_location
FROM (
    SELECT DISTINCT
        r.city                          AS city,
        r.fsa                           AS fsa,
        c.population                    AS population,
        c.average_age                   AS average_age,
        c.median_income                 AS median_income
    FROM silver.restaurants r
    JOIN (
        SELECT
            fsa,
            MAX(CASE WHEN variable = 'population' THEN value END) AS population,
            MAX(CASE WHEN variable = 'average_age' THEN value END) AS average_age,
            MAX(CASE WHEN variable = 'median_income' THEN value END) AS median_income
        FROM silver.census_2021
        GROUP BY fsa
    ) c ON r.fsa = c.fsa
) t;
GO

-- =============================================================================
-- Create Dimension Table: gold.dim_category
-- =============================================================================

IF OBJECT_ID('gold.dim_category', 'U') IS NOT NULL
    DROP TABLE gold.dim_category;
GO

SELECT 
    ROW_NUMBER() OVER (ORDER BY restaurant_key) AS category_key, -- Surrogate key
    restaurant_key, -- Surrogate key linking the review to dim_restaurant
    category
INTO gold.dim_category
FROM (
    SELECT 
        r.restaurant_key   AS restaurant_key,
        c.category         AS category
    FROM gold.dim_restaurant r
    JOIN silver.categories c
    ON r.yelp_id = c.yelp_id
) t;
GO

-- =============================================================================
-- Create Fact Table: gold.fact_restaurants
-- =============================================================================

IF OBJECT_ID('gold.fact_restaurants', 'U') IS NOT NULL
    DROP TABLE gold.fact_restaurants;
GO

SELECT
    dr.restaurant_key      AS restaurant_key,
    dl.location_key        AS location_key,
    r.google_rating        AS google_rating,
    r.yelp_rating          AS yelp_rating,
    r.google_price_level   AS google_price_level,
    r.yelp_price_level     AS yelp_price_level
INTO gold.fact_restaurants
FROM silver.restaurants r
JOIN gold.dim_restaurant dr 
    ON (
        (r.google_id = dr.google_id AND r.source IN ('google', 'both'))
        OR 
        (r.yelp_id = dr.yelp_id AND r.source = 'yelp')
    )
LEFT JOIN gold.dim_location dl
    ON r.fsa = dl.fsa;
GO

-- =============================================================================
-- Create Fact Table: gold.fact_reviews
-- =============================================================================

IF OBJECT_ID('gold.fact_reviews', 'U') IS NOT NULL
    DROP TABLE gold.fact_reviews;
GO

SELECT
    ROW_NUMBER() OVER (ORDER BY restaurant_key) AS review_key, -- Surrogate key
    dr.restaurant_key    AS restaurant_key, -- Surrogate key linking the review to dim_restaurant
    r.author_name        AS author_name,
    r.rating             AS rating,
    r.text               AS text,
    r.review_time        AS review_time
INTO gold.fact_reviews
FROM silver.google_reviews r
JOIN gold.dim_restaurant dr
    ON r.google_id = dr.google_id;
GO
