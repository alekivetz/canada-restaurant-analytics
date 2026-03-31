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

CREATE TABLE silver.google_restaurants (
    restaurant_id         NVARCHAR(50) PRIMARY KEY,
    name                  NVARCHAR(255),
    rating                DECIMAL(3,2),
    user_ratings_total    INT,
    price_level           INT,
    lat                   DECIMAL(9,6),
    lon                   DECIMAL(9,6),
    city                  NVARCHAR(50),
    fsa                   NVARCHAR(3) 
);
GO

IF OBJECT_ID('silver.google_reviews', 'U') IS NOT NULL
    DROP TABLE silver.google_reviews;
GO

CREATE TABLE silver.google_reviews (
    review_id             INT IDENTITY(1, 1) PRIMARY KEY,
    restaurant_id         NVARCHAR(255),
    author_name           NVARCHAR(255),
    rating                DECIMAL(3,2),
    text                  NVARCHAR(MAX),
    review_time           DATETIME,
    city                  NVARCHAR(50)
);
GO

IF OBJECT_ID('silver.google_categories', 'U') IS NOT NULL
    DROP TABLE google_categories;
GO

CREATE TABLE silver.google_categories (
    restaurant_id   NVARCHAR(255),
    category        NVARCHAR(100)
);

IF OBJECT_ID('silver.census_2021', 'U') IS NOT NULL
    DROP TABLE silver.census_2021;
GO

CREATE TABLE silver.census_2021 (
    geo_code              NVARCHAR(50),
    variable              NVARCHAR(50),
    value                 DECIMAL(18,2)
);
GO