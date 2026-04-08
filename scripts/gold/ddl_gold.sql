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
-- Central dimension table linking all fact tables and category data
-- =============================================================================

IF OBJECT_ID('gold.dim_restaurant', 'U') IS NOT NULL
    DROP TABLE gold.dim_restaurant;
GO

CREATE TABLE gold.dim_restaurant (
    restaurant_key     INT IDENTITY(1,1) PRIMARY KEY,
    google_id          NVARCHAR(50),
    yelp_id            NVARCHAR(50),
    name               NVARCHAR(255),
    lat                DECIMAL(9,6),
    lon                DECIMAL(9,6),
    source             NVARCHAR(10),
    match_method       NVARCHAR(20)
);
GO

-- =============================================================================
-- Create Dimension Table: gold.dim_location
-- Location and demographic data at the FSA level, enriched with census data
-- =============================================================================

IF OBJECT_ID('gold.dim_location', 'U') IS NOT NULL
    DROP TABLE gold.dim_location;
GO

CREATE TABLE gold.dim_location (
    location_key       INT IDENTITY(1,1) PRIMARY KEY,
    city               NVARCHAR(50),
    fsa                NVARCHAR(3),
    population         DECIMAL(18,2),
    average_age        DECIMAL(18,2),
    median_income      DECIMAL(18,2)
);
GO

-- =============================================================================
-- Create Dimension Table: gold.dim_category
-- Restaurant category tags sourced from Yelp Fusion API
-- =============================================================================

IF OBJECT_ID('gold.dim_category', 'U') IS NOT NULL
    DROP TABLE gold.dim_category;
GO

CREATE TABLE gold.dim_category (
    category_key       INT IDENTITY(1,1) PRIMARY KEY,
    restaurant_key     INT,
    category           NVARCHAR(100)
);
GO

-- =============================================================================
-- Create Fact Table: gold.fact_restaurants
-- Analytical metrics for each restaurant including ratings and price levels
-- =============================================================================

IF OBJECT_ID('gold.fact_restaurants', 'U') IS NOT NULL
    DROP TABLE gold.fact_restaurants;
GO

CREATE TABLE gold.fact_restaurants (
    restaurant_key       INT,
    location_key         INT,
    google_rating        DECIMAL(3,2),
    yelp_rating          DECIMAL(3,2),
    google_price_level   NVARCHAR(5),
    yelp_price_level     NVARCHAR(5)
);
GO

-- =============================================================================
-- Create Fact Table: gold.fact_reviews
-- Individual customer reviews from Google Places API
-- =============================================================================

IF OBJECT_ID('gold.fact_reviews', 'U') IS NOT NULL
    DROP TABLE gold.fact_reviews;
GO

CREATE TABLE gold.fact_reviews (
    review_key          INT IDENTITY(1,1) PRIMARY KEY,
    restaurant_key      INT,
    author_name         NVARCHAR(255),
    rating              DECIMAL(3,2),
    text                NVARCHAR(MAX),
    review_time         DATETIME
);
GO