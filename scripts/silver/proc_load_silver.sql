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
            google_id,
            name,
            rating,
            user_ratings_total,
            price_level,
            city,
            lat,
            lon,
            fsa,
            phone_number
        )

        SELECT 
            google_id,
            NULLIF(TRIM(name), '') AS name,
            rating,
            user_ratings_total,
            CASE price_level
                WHEN 1 THEN '$'
                WHEN 2 THEN '$$'
                WHEN 3 THEN '$$$'
                WHEN 4 THEN '$$$$'
                ELSE 'N/A'
            END AS price_level, -- Standardize price level to match yelp
            city,
            lat,
            lon,
            fsa,
            dbo.strip_non_numeric(phone_number) AS phone_number
        FROM (
            SELECT 
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY google_id
                    ORDER BY price_level DESC 
                ) AS rn
            FROM bronze.google_restaurants
            ) t
            WHERE rn = 1; -- Select restaurant with most expensive price for duplicate ids
        
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
            yelp_id,
            name,
            rating,
            price_level,
            city,
            lat,
            lon,
            fsa,
            phone_number
        )

        SELECT 
            yelp_id,
            NULLIF(TRIM(name), '') AS name,
            rating,
            NULLIF(price_level, 'N/A') AS price_level,
            CASE 
                WHEN city IN ('Etobicoke', 'Scarborough', 'North York', 'East York', 'Markham', 'Vaughan', 'Mississauga', 'Thornhill', 'Concord', 'York') THEN 'Toronto'
                WHEN city IN ('Burnaby', 'Richmond') THEN 'Vancouver'
                WHEN city IN ('St-Leonard', 'Saint-Leonard', 'Saint-Léonard', 'Montreal-Nord', 'Anjou', 'Verdun', 'Westmount') THEN 'Montreal'
                WHEN city = 'Sherwood Park' THEN 'Edmonton'
                WHEN city = 'Ottawa' THEN NULL
                ELSE city
            END AS city,
            lat,
            lon,
            NULLIF(TRIM(fsa), 'n/a') AS fsa,
            dbo.strip_non_numeric(phone_number) AS phone_number
        FROM (
            SELECT 
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY yelp_id
                    ORDER BY LEN(price_level) DESC
                ) AS rn
            FROM bronze.yelp_restaurants
            ) t
            WHERE rn = 1; -- Select restaurant with most expensive price for duplicate ids
        
        SET @end_time = GETDATE();
	    PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';


        PRINT ''
		PRINT '-------------------------------------------';
		PRINT 'Loading Overall Restaurant Table';
		PRINT '-------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.restaurants';
		TRUNCATE TABLE silver.restaurants;

		PRINT '>> Inserting Data Into: restaurants';
		INSERT INTO silver.restaurants (
            google_id,
            yelp_id,
            name,
            google_rating,
            yelp_rating,
            google_price_level,
            yelp_price_level,
            city,
            lat,
            lon,
            fsa,
            source
        )

        -- Restaurants that are both Google and Yelp
        SELECT
            google_id,
            yelp_id,
            name,
            google_rating,
            yelp_rating,
            google_price_level,
            yelp_price_level,
            city,
            lat,
            lon,
            fsa,
            'both' AS source
        FROM (
            SELECT
                g.google_id             AS google_id,
                y.yelp_id               AS yelp_id,
                g.name                  AS name,
                g.rating                AS google_rating,
                y.rating                AS yelp_rating,
                g.price_level           AS google_price_level,
                y.price_level           AS yelp_price_level,
                g.city                  AS city,
                g.lat                   AS lat,
                g.lon                   AS lon,
                g.fsa                   AS fsa,
                ROW_NUMBER() OVER (
                            PARTITION BY g.google_id
                            ORDER BY 
                                CASE WHEN g.phone_number = y.phone_number THEN 0 ELSE 1 END,
                                ABS(g.lat - y.lat) + ABS(g.lon - y.lon)
                        ) AS rn
            FROM silver.google_restaurants g
                JOIN silver.yelp_restaurants y
                ON g.city = y.city
                    AND (
                            (g.phone_number IS NOT NULL
                    AND y.phone_number IS NOT NULL
                    AND g.phone_number = y.phone_number)
                    OR
                    (
                    (g.phone_number IS NULL OR y.phone_number IS NULL)
                    AND DIFFERENCE(g.name, y.name) = 4
                    AND ABS(g.lat - y.lat) < 0.0005
                    AND ABS(g.lon - y.lon) < 0.0005
                    )
                )
            ) t
        WHERE rn = 1

        UNION ALL

        -- Restaurants that are only Google
        SELECT
            g.google_id                 AS google_id,
            NULL                        AS yelp_id,
            g.name                      AS name,    
            g.rating                    AS google_rating,
            NULL                        AS yelp_rating,
            g.price_level               AS google_price_level,
            NULL                        AS yelp_price_level,    
            g.city                      AS city,    
            g.lat                       AS lat, 
            g.lon                       AS lon, 
            g.fsa                       AS fsa,
            'google'                    AS source
        FROM silver.google_restaurants g
            LEFT JOIN silver.yelp_restaurants y
            ON g.city = y.city
            AND (
                (g.phone_number IS NOT NULL
                AND y.phone_number IS NOT NULL
                AND g.phone_number = y.phone_number)
                OR
                (
                (g.phone_number IS NULL OR y.phone_number IS NULL)
                AND DIFFERENCE(g.name, y.name) = 4
                AND ABS(g.lat - y.lat) < 0.0005
                AND ABS(g.lon - y.lon) < 0.0005
                )
            )
        WHERE y.yelp_id IS NULL

        UNION ALL

        -- Restaurants that are only Yelp
        SELECT
            NULL                    AS google_id,
            y.yelp_id               AS yelp_id,
            y.name                  AS name,
            NULL                    AS google_rating,
            y.rating                AS yelp_rating,
            NULL                    AS google_price_level,
            y.price_level           AS yelp_price_level,    
            y.city                  AS city,    
            y.lat                   AS lat, 
            y.lon                   AS lon,
            y.fsa                   AS fsa,
            'yelp'                  AS source
        FROM silver.yelp_restaurants y
            LEFT JOIN silver.google_restaurants g
            ON y.city = g.city
            AND (
                (g.phone_number IS NOT NULL
                AND y.phone_number IS NOT NULL
                AND g.phone_number = y.phone_number)
                OR
                (
                (g.phone_number IS NULL OR y.phone_number IS NULL)
                AND DIFFERENCE(g.name, y.name) = 4
                AND ABS(g.lat - y.lat) < 0.0005
                AND ABS(g.lon - y.lon) < 0.0005
                )
            )
        WHERE g.google_id IS NULL;

        SET @end_time = GETDATE();
	    PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        PRINT ''
        PRINT '-------------------------------------------';
        PRINT 'Loading Categories Table';
        PRINT '-------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.categories';
        TRUNCATE TABLE silver.categories;

        PRINT '>> Inserting Data Into: categories';
        INSERT INTO silver.categories (
            yelp_id,
            category
        )

        SELECT
            yelp_id,
            TRIM(value) AS category
        FROM bronze.yelp_restaurants
        CROSS APPLY STRING_SPLIT(categories, ',')
        WHERE TRIM(value) != '';

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';


        PRINT ''
		PRINT '-------------------------------------------';
		PRINT 'Loading Google Reviews Table';
		PRINT '-------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.google_reviews';
		TRUNCATE TABLE silver.google_reviews;

		PRINT '>> Inserting Data Into: google_reviews';
		INSERT INTO silver.google_reviews (
            google_id,
            author_name,
            rating,
            text,
            review_time
        )

        SELECT
            google_id,
            NULLIF(TRIM(author_name), '') AS author_name,
            rating,
            text,
            DATEADD(SECOND, review_time, CAST('1970-01-01' AS DATETIME)) AS review_time
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY google_id, author_name
                    ORDER BY review_time DESC
                ) AS rn
            FROM bronze.google_reviews
            ) t
            WHERE rn = 1; -- Select review with most recent time for duplicate restaurant/author

        SET @end_time = GETDATE();
	    PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';


        PRINT ''
		PRINT '-------------------------------------------';
		PRINT 'Loading Statistics Canada Census Table';
		PRINT '-------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.census_2021';
		TRUNCATE TABLE silver.census_2021;

		PRINT '>> Inserting Data Into: census_2021';
		INSERT INTO silver.census_2021 (
            fsa,
            variable,
            value
        )

        SELECT 
            NULLIF(TRIM(fsa), '') AS fsa,
            NULLIF(TRIM(variable), '') AS variable, 
            value
        FROM bronze.census_2021;

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
