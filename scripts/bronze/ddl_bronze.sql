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

CREATE TABLE bronze.google_restaurants (
    restaurant_id         NVARCHAR(50),
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

IF OBJECT_ID('bronze.google_reviews', 'U') IS NOT NULL
    DROP TABLE bronze.google_reviews;
GO

CREATE TABLE bronze.google_reviews (
    restaurant_id         NVARCHAR(255),
    author_name           NVARCHAR(255),
    rating                DECIMAL(3,2),
    text                  NVARCHAR(MAX),
    review_time           BIGINT,
    city                  NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.google_categories', 'U') IS NOT NULL
    DROP TABLE bronze.google_categories;
GO

CREATE TABLE bronze.google_categories (
    restaurant_id   NVARCHAR(255),
    category        NVARCHAR(100)
);

IF OBJECT_ID('bronze.census_2021', 'U') IS NOT NULL
    DROP TABLE bronze.census_2021;
GO

CREATE TABLE bronze.census_2021 (
    fsa                   NVARCHAR(3),       
    variable              NVARCHAR(50),
    value                 DECIMAL(18,2)
);
GO


