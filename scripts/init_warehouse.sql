/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO

/*
===============================================================================
User Defined Function: strip_non_numeric
===============================================================================
Function Purpose:
    Strips all non-numeric characters from a string, returning only digits.
    Used to standardize phone numbers from multiple sources into a consistent
    format for matching and comparison.

Parameters:
    @input NVARCHAR(50) - The string to strip non-numeric characters from

Returns:
    NVARCHAR(50) - The input string with all non-numeric characters removed

Usage Example:
    SELECT dbo.strip_non_numeric('+1 (780) 123-4567')  -- Returns 17801234567
    SELECT dbo.strip_non_numeric('+17801234567')        -- Returns 17801234567
===============================================================================
*/

GO

CREATE OR ALTER FUNCTION dbo.strip_non_numeric(@input NVARCHAR(50))
RETURNS NVARCHAR(50)
AS
BEGIN
    DECLARE @output NVARCHAR(50) = ''
    DECLARE @i INT = 1
    WHILE @i <= LEN(@input)
    BEGIN
        IF SUBSTRING(@input, @i, 1) LIKE '[0-9]'
            SET @output = @output + SUBSTRING(@input, @i, 1)
        SET @i = @i + 1
    END

    -- If longer than 11 digits, truncate to first 11
    IF LEN(@output) > 11
        SET @output = LEFT(@output, 11)

    -- If not 11 digits after truncation, return NULL
    IF LEN(@output) != 11
        SET @output = NULL

    RETURN @output
END
