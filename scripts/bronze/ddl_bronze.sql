/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the bronze schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of bronze Tables
===============================================================================
*/

USE DataWarehouse;
GO

IF OBJECT_ID('bronze.google_restaurants', 'U') IS NOT NULL
    DROP TABLE bronze.google_restaurants;
GO

-- Restaurant locations and metadata from Google Places API
CREATE TABLE bronze.google_restaurants (
    google_id             NVARCHAR(50),
    name                  NVARCHAR(255),
    rating                DECIMAL(3,2),
    user_ratings_total    INT,
    price_level           INT,
    city                  NVARCHAR(50),
    lat                   DECIMAL(9,6),
    lon                   DECIMAL(9,6),
    fsa                   NVARCHAR(3),
    phone_number          NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.google_reviews', 'U') IS NOT NULL
    DROP TABLE bronze.google_reviews;
GO

-- Customer reviews pulled from Google Places Details API
CREATE TABLE bronze.google_reviews (
    google_id             NVARCHAR(255),
    author_name           NVARCHAR(255),
    rating                DECIMAL(3,2),
    text                  NVARCHAR(MAX),
    review_time           BIGINT
);
GO

IF OBJECT_ID('bronze.yelp_restaurants', 'U') IS NOT NULL
    DROP TABLE bronze.yelp_restaurants;
GO

-- Restaurant locations and metadata from Yelp Fusion API
CREATE TABLE bronze.yelp_restaurants (
    yelp_id               NVARCHAR(50),
    name                  NVARCHAR(255),
    rating                DECIMAL(3,2),
    categories            NVARCHAR(MAX),
    price_level           NVARCHAR(5),
    city                  NVARCHAR(50),
    lat                   DECIMAL(9,6),
    lon                   DECIMAL(9,6),
    fsa                   NVARCHAR(3),
    phone_number          NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.census_2021', 'U') IS NOT NULL
    DROP TABLE bronze.census_2021;
GO

-- Statistics Canada 2021 census data at the FSA level
CREATE TABLE bronze.census_2021 (
    fsa                   NVARCHAR(3),       
    variable              NVARCHAR(50),
    value                 DECIMAL(18,2)
);
GO


