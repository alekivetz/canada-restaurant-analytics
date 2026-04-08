/*
===============================================================================
Stored Procedure: Load Gold Layer (Silver -> Gold)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL process to populate the gold schema
    tables from the silver schema, implementing a star schema for analytical
    reporting and business intelligence.

    Actions Performed:
        - Truncates gold tables
        - Inserts transformed and enriched data from silver into gold tables
        - Builds dimension and fact tables following star schema design

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC gold.load_gold;

Notes:
    - Requires silver layer tables to be populated before running
    - Run after EXEC silver.load_silver
===============================================================================
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE gold.load_gold AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '===========================================';
		PRINT 'Loading Gold Layer';
		PRINT '===========================================';
        

        PRINT ''
		PRINT '-------------------------------------------';
		PRINT 'Loading Restaurant Dimension Table';
		PRINT '-------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: gold.dim_restaurant';
		TRUNCATE TABLE gold.dim_restaurant;

		PRINT '>> Inserting Data Into: dim_restaurant';

        INSERT INTO gold.dim_restaurant (
            google_id,
            yelp_id,
            name,
            lat,
            lon,
            source, 
            match_method
        )

        SELECT
            google_id,
            yelp_id,
            name,
            lat,
            lon,
            source,
            match_method
        FROM silver.restaurants;

        SET @end_time = GETDATE();
	    PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';


        PRINT ''
		PRINT '-------------------------------------------';
		PRINT 'Loading Location Dimension Table';
		PRINT '-------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: gold.dim_location';
		TRUNCATE TABLE gold.dim_location;

		PRINT '>> Inserting Data Into: dim_location';
		INSERT INTO gold.dim_location (
            city,
            fsa,
            population,
            average_age,
            median_income
        )

        SELECT DISTINCT
            r.city               AS city,
            c.fsa                AS fsa,
            c.population         AS population,
            c.average_age        AS average_age,
            c.median_income      AS median_income
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
        WHERE r.city IS NOT NULL;
        
        SET @end_time = GETDATE();
	    PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';


        PRINT ''
		PRINT '-------------------------------------------';
		PRINT 'Loading Category Dimension Table';
		PRINT '-------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: gold.dim_category';
		TRUNCATE TABLE gold.dim_category;

		PRINT '>> Inserting Data Into: dim_category';
		INSERT INTO gold.dim_category (
            restaurant_key,
            category
        )

        SELECT
            r.restaurant_key     AS restaurant_key,
            c.category           AS category
        FROM gold.dim_restaurant r
        JOIN silver.categories c
        ON r.yelp_id = c.yelp_id;
        
        SET @end_time = GETDATE();
	    PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        PRINT ''
        PRINT '-------------------------------------------';
        PRINT 'Loading Restaurants Fact Table';
        PRINT '-------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: gold.fact_restaurants';
        TRUNCATE TABLE gold.fact_restaurants;

        PRINT '>> Inserting Data Into: fact_restaurants';
        INSERT INTO gold.fact_restaurants (
            restaurant_key,
            location_key,
            google_rating,
            yelp_rating,
            google_price_level,
            yelp_price_level
        )

        SELECT
            dr.restaurant_key       AS restaurant_key,
            dl.location_key         AS location_key,
            r.google_rating         AS google_rating,
            r.yelp_rating           AS yelp_rating,
            r.google_price_level    AS google_price_level,
            r.yelp_price_level      AS yelp_price_level
        FROM silver.restaurants r
        JOIN gold.dim_restaurant dr
            ON (
                (r.google_id = dr.google_id AND r.source IN ('google', 'both'))
                OR
                (r.yelp_id = dr.yelp_id AND r.source = 'yelp')
            )
        LEFT JOIN gold.dim_location dl
            ON r.fsa = dl.fsa;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';


        PRINT ''
		PRINT '-------------------------------------------';
		PRINT 'Loading Reviews Fact Table';
		PRINT '-------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: gold.fact_reviews';
		TRUNCATE TABLE gold.fact_reviews;

		PRINT '>> Inserting Data Into: fact_reviews';
		INSERT INTO gold.fact_reviews (
            restaurant_key,
            author_name,
            rating,
            text,
            review_time
        )

        SELECT
            dr.restaurant_key    AS restaurant_key,
            r.author_name        AS author_name,
            r.rating             AS rating,
            r.text               AS text,
            r.review_time        AS review_time
        FROM silver.google_reviews r
        JOIN gold.dim_restaurant dr
            ON r.google_id = dr.google_id;
        
        SET @end_time = GETDATE();
	    PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		SET @batch_end_time = GETDATE();
        PRINT ''
		PRINT '===========================================';
		PRINT 'Loading Gold Layer is Complete';
		PRINT '>> Total Load Duration: ' + CAST (DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '===========================================';
	END TRY

	BEGIN CATCH
        PRINT ''
		PRINT '===========================================';
		PRINT 'ERROR OCCURED DURING LOADING GOLD LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '===========================================';
	END CATCH
END

