/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the silver schema tables from the bronze schema.
	Actions Performed:
		- Truncates silver tables.
		- Inserts transformed and cleansed data from bronze into silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '===========================================';
		PRINT 'Loading Silver Layer';
		PRINT '===========================================';
        
        PRINT ''
		PRINT '-------------------------------------------';
		PRINT 'Loading Google Restaurant Table';
		PRINT '-------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.google_restaurants';
		TRUNCATE TABLE silver.google_restaurants;

		PRINT '>> Inserting Data Into: google_restaurants';
		INSERT INTO silver.google_restaurants (
            restaurant_id,
            name,
            rating,
            user_ratings_total,
            price_level,
            city,
            lat,
            lon,
            fsa
        )

        SELECT 
            restaurant_id,
            NULLIF(TRIM(name), '') AS name,
            rating,
            user_ratings_total,
            price_level,
            city,
            lat,
            lon,
            fsa
        FROM (
            SELECT 
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY restaurant_id
                    ORDER BY price_level DESC
                ) AS rn
            FROM bronze.google_restaurants
            ) t
            WHERE rn = 1; -- Select restaurant with most expensive price for duplicate ids
        
        SET @end_time = GETDATE();
	    PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        PRINT ''
		PRINT '-------------------------------------------';
		PRINT 'Loading Google Reviews Table';
		PRINT '-------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.google_reviews';
		TRUNCATE TABLE silver.google_reviews;

		PRINT '>> Inserting Data Into: google_reviews';
		INSERT INTO silver.google_reviews (
            restaurant_id,
            author_name,
            rating,
            text,
            review_time
        )

        SELECT
            restaurant_id,
            NULLIF(TRIM(author_name), '') AS author_name,
            rating,
            text,
            DATEADD(SECOND, review_time, '1970-01-01') AS review_time
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY restaurant_id, author_name
                    ORDER BY review_time DESC
                ) AS rn
            FROM bronze.google_reviews
            ) t
            WHERE rn = 1; -- Select review with most recent time for duplicate restaurant/author

        SET @end_time = GETDATE();
	    PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        PRINT ''
		PRINT '-------------------------------------------';
		PRINT 'Loading Yelp Restaurant Table';
		PRINT '-------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.yelp_restaurants';
		TRUNCATE TABLE silver.yelp_restaurants;

		PRINT '>> Inserting Data Into: yelp_restaurants';
		INSERT INTO silver.yelp_restaurants (
            restaurant_id,
            name,
            rating,
            categories,
            price_level,
            city,
            lat,
            lon,
            fsa
        )

        SELECT 
            restaurant_id,
            NULLIF(TRIM(name), '') AS name,
            rating,
            categories,
            price_level,
            city,
            lat,
            lon,
            fsa
        FROM (
            SELECT 
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY restaurant_id
                    ORDER BY price_level DESC
                ) AS rn
            FROM bronze.yelp_restaurants
            ) t
            WHERE rn = 1; -- Select restaurant with most expensive price for duplicate ids
        
        SET @end_time = GETDATE();
	    PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		SET @batch_end_time = GETDATE();
        PRINT ''
		PRINT '===========================================';
		PRINT 'Loading Silver Layer is Complete';
		PRINT '>> Total Load Duration: ' + CAST (DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '===========================================';
	END TRY

	BEGIN CATCH
        PRINT ''
		PRINT '===========================================';
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '===========================================';
	END CATCH
END