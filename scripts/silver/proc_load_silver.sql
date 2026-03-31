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
		PRINT 'Loading Places Table';
		PRINT '-------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.places';
		TRUNCATE TABLE silver.places;

		PRINT '>> Inserting Data Into: places';
		INSERT INTO silver.places (
            place_id,
            name,
            rating,
            user_ratings_total,
            price_level,
            lat,
            lon,
            city
        )

        SELECT 
            place_id,
            NULLIF(TRIM(name), '') AS name,
            rating,
            user_ratings_total,
            price_level,
            lat,
            lon,
            UPPER(LEFT(city,1)) + LOWER(SUBSTRING(city,2,LEN(city))) AS city
        FROM (
            SELECT 
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY place_id
                    ORDER BY user_ratings_total DESC
                ) AS rn
            FROM bronze.places
            ) t
            WHERE rn = 1; -- Select place with most user ratings for duplicate ids
        
        SET @end_time = GETDATE();
	    PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        PRINT ''
		PRINT '-------------------------------------------';
		PRINT 'Loading Reviews Table';
		PRINT '-------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.reviews';
		TRUNCATE TABLE silver.reviews;

		PRINT '>> Inserting Data Into: reviews';
		INSERT INTO silver.reviews (
            place_id,
            author_name,
            rating,
            text,
            review_time
        )

        SELECT
            place_id,
            NULLIF(TRIM(author_name), '') AS author_name,
            rating,
            text,
            DATEADD(SECOND, review_time, '1970-01-01') AS review_time
        FROM bronze.reviews;

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

EXEC silver.load_silver;