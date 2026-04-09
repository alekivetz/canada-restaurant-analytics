/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the silver schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of silver tables
===============================================================================
*/

USE DataWarehouse;
GO

IF OBJECT_ID('silver.google_restaurants', 'U') IS NOT NULL
    DROP TABLE silver.google_restaurants;
GO

-- Restaurant locations and metadata from Google Places API
CREATE TABLE silver.google_restaurants (
    google_id             NVARCHAR(50), 
    name                  NVARCHAR(255),
    rating                DECIMAL(3,2),
    user_ratings_total    INT,
    price_level           NVARCHAR(5),
    lat                   DECIMAL(9,6),
    lon                   DECIMAL(9,6),
    city                  NVARCHAR(50),
    fsa                   NVARCHAR(3),
    phone_number          NVARCHAR(50)
);
GO

IF OBJECT_ID('silver.google_reviews', 'U') IS NOT NULL
    DROP TABLE silver.google_reviews;
GO

-- Customer reviews pulled from Google Places Details API
CREATE TABLE silver.google_reviews (
    google_id             NVARCHAR(255),
    author_name           NVARCHAR(255),
    rating                DECIMAL(3,2),
    text                  NVARCHAR(MAX),
    review_time           DATETIME,
);
GO


IF OBJECT_ID('silver.yelp_restaurants', 'U') IS NOT NULL
    DROP TABLE silver.yelp_restaurants;
GO

-- Restaurant locations and metadata from Yelp Fusion API
CREATE TABLE silver.yelp_restaurants (
    yelp_id               NVARCHAR(50),
    name                  NVARCHAR(255),
    rating                DECIMAL(3,2),
    price_level           NVARCHAR(5),
    city                  NVARCHAR(50),
    lat                   DECIMAL(9,6),
    lon                   DECIMAL(9,6),
    fsa                   NVARCHAR(3),
    phone_number          NVARCHAR(50)
);
GO

IF OBJECT_ID('silver.restaurants', 'U') IS NOT NULL
    DROP TABLE silver.restaurants;
GO

-- Merged restaurant table with Google and Yelp data
CREATE TABLE silver.restaurants (
    google_id             NVARCHAR(50),
    yelp_id               NVARCHAR(50),
    name                  NVARCHAR(255),
    google_rating         DECIMAL(3,2),
    yelp_rating           DECIMAL(3,2),
    google_price_level    NVARCHAR(5),
    yelp_price_level      NVARCHAR(5),
    price_level           NVARCHAR(5),
    city                  NVARCHAR(50),
    lat                   DECIMAL(9,6),
    lon                   DECIMAL(9,6),
    fsa                   NVARCHAR(3),
    source                NVARCHAR(10),
    match_method          NVARCHAR(20)
);
GO

IF OBJECT_ID('silver.categories', 'U') IS NOT NULL
    DROP TABLE silver.categories;
GO

-- Categories table
CREATE TABLE silver.categories (
    yelp_id               NVARCHAR(50),
    category              NVARCHAR(100)
);
GO

IF OBJECT_ID('silver.census_2021', 'U') IS NOT NULL
    DROP TABLE silver.census_2021;
GO

-- Statistics Canada 2021 census data at the FSA level
CREATE TABLE silver.census_2021 (
    fsa                   NVARCHAR(3),
    variable              NVARCHAR(50),
    value                 DECIMAL(18,2)
);
GO