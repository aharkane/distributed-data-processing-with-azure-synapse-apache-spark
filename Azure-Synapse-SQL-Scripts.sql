/*******************************************************************************
 * AZURE SYNAPSE ANALYTICS - COMPLETE SQL PORTFOLIO WITH TUTORIAL COMMENTS
 * 
 * This file consolidates all Azure Synapse SQL operations with detailed
 * explanatory comments to serve as a learning reference and tutorial guide.
 *
 * Author: Your Name
 * Date: November 2025
 * Certification: Azure DP-203 Data Engineer Associate
 ******************************************************************************/

-------------------------------------------------------------------------------
-- SECTION 1: SERVERLESS SQL POOL - DATA EXPLORATION
-------------------------------------------------------------------------------

-- Query CSV files directly from Azure Data Lake using OPENROWSET
-- OPENROWSET allows querying external files without creating tables
SELECT TOP 100 *
FROM OPENROWSET(
    BULK 'https://datalaketx24ncr.dfs.core.windows.net/files/product_data/products.csv',
    FORMAT = 'CSV',  -- Specifies the file format
    PARSER_VERSION = '2.0',  -- Uses CSV parser version 2.0 for improved parsing
    HEADER_ROW = TRUE  -- Indicates that the first row contains column headers
) AS [result];

-- Aggregate data by category to count products
-- Shows grouping and counting directly on external data without loading it
SELECT TOP 10
    category,
    COUNT(*) AS productCount
FROM OPENROWSET(
    BULK 'https://datalaketx24ncr.dfs.core.windows.net/files/product_data/products.csv',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE
) AS result
GROUP BY category
ORDER BY productCount DESC;


-------------------------------------------------------------------------------
-- SECTION 2: QUERYING MULTIPLE FILE FORMATS (CSV, Parquet, JSON)
-------------------------------------------------------------------------------

-- Query CSV files with wildcard pattern to read multiple files at once
-- The ** pattern recursively searches all subdirectories
SELECT TOP 100 *
FROM OPENROWSET(
    BULK 'https://datalake6tbnq4u.dfs.core.windows.net/files/sales/csv/**',  -- ** reads all CSV files recursively
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE  -- First row in each CSV file contains column names
) AS [result];

-- Query CSV with explicit schema definition using WITH clause
-- Defining schema explicitly improves performance and ensures correct data types
SELECT TOP 100 *
FROM OPENROWSET(
    BULK 'https://datalakepbt1exd.dfs.core.windows.net/files/sales/csv/',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0'
) WITH (
    SalesOrderNumber VARCHAR(10) COLLATE Latin1_General_100_BIN2_UTF8,  -- UTF-8 collation for text columns
    SalesOrderLineNumber INT,
    OrderDate DATE,
    CustomerName VARCHAR(25) COLLATE Latin1_General_100_BIN2_UTF8,
    EmailAddress VARCHAR(50) COLLATE Latin1_General_100_BIN2_UTF8,
    Item VARCHAR(30) COLLATE Latin1_General_100_BIN2_UTF8,
    Quantity INT,
    UnitPrice DECIMAL(18,2),  -- Use DECIMAL for monetary values
    TaxAmount DECIMAL(18,2)
) AS result;

-- Query Parquet files with partition pruning for better performance
-- filepath(1) refers to the first folder level in the path (e.g., year=2019)
-- Partition pruning = filtering on partition columns to read only relevant files
SELECT
    YEAR(OrderDate) AS OrderYear,
    COUNT(*) AS OrderedItems
FROM OPENROWSET(
    BULK 'https://datalakepbt1exd.dfs.core.windows.net/files/sales/parquet/year=*/',  -- * is a wildcard for partition values
    FORMAT = 'PARQUET'
) AS [result]
WHERE [result].filepath(1) IN ('2019', '2020')  -- Partition pruning: Only read data from 2019 and 2020 partitions
GROUP BY YEAR(OrderDate)
ORDER BY OrderYear ASC;

-- Query JSON files and extract properties using JSON_VALUE
-- JSON files require special field terminators to read correctly
SELECT TOP 100
    JSON_VALUE(doc, '$.SalesOrderNumber') AS OrderNumber,  -- $ is the root element, then property path
    JSON_VALUE(doc, '$.CustomerName') AS Customer,
    JSON_VALUE(doc, '$.Item') AS Item,
    JSON_VALUE(doc, '$.Quantity') AS Quantity,
    doc AS FullDocument  -- Keep the full JSON document for reference
FROM OPENROWSET(
    BULK 'https://datalakepbt1exd.dfs.core.windows.net/files/sales/json/**',
    FORMAT = 'CSV',  -- Read JSON as CSV with special terminators
    FIELDTERMINATOR = '0x0b',  -- Hex value for vertical tab (not present in JSON)
    FIELDQUOTE = '0x0b',
    ROWTERMINATOR = '0x0b'
) WITH (doc NVARCHAR(MAX)) AS rows;  -- Read entire row as single NVARCHAR column


-------------------------------------------------------------------------------
-- SECTION 3: DATABASE AND EXTERNAL RESOURCES SETUP
-------------------------------------------------------------------------------

-- Create a Database in serverless SQL pool (only stores metadata, not actual data)
-- Serverless pools are compute-only; data stays in Azure Data Lake
CREATE DATABASE Sales
    COLLATE Latin1_General_100_BIN2_UTF8;  -- Defines collation for text data storage (UTF-8 with binary sorting)
GO;

-- Switch to the newly created database
-- Note: USE statement works in dedicated pools but context switching in serverless pools
USE Sales;
GO;

-- Create an External Data Source: Defines the base storage path in ADLS Gen2 where data files are stored
-- This allows you to reference files using relative paths instead of full URLs
CREATE EXTERNAL DATA SOURCE sales_data WITH (
    LOCATION = 'https://datalake6tbnq4u.dfs.core.windows.net/files/'  -- Base path for data storage
);
GO;

-- Create an External File Format: Defines the file format and compression used for data storage
-- This is reusable for multiple external tables using the same format
CREATE EXTERNAL FILE FORMAT ParquetFormat WITH (
    FORMAT_TYPE = PARQUET,  -- Specifies the Parquet file format
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'  -- Uses Snappy compression for efficiency
);
GO;

-- Create external file format for CSV files
CREATE EXTERNAL FILE FORMAT CsvFormat WITH (
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS(
        FIELD_TERMINATOR = ',',  -- Comma-separated values
        STRING_DELIMITER = '"',  -- Text values enclosed in quotes
        FIRST_ROW = 2  -- Skip header row (row 1)
    )
);
GO;


-------------------------------------------------------------------------------
-- SECTION 4: QUERYING WITH EXTERNAL DATA SOURCES
-------------------------------------------------------------------------------

-- Query CSV using external data source to simplify paths
-- Now you only need to specify the relative path from the data source
SELECT *
FROM OPENROWSET(
    BULK 'sales/csv/*.csv',  -- Relative path from the external data source
    DATA_SOURCE = 'sales_data',  -- References the external storage location defined earlier
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE
) AS orders;

-- Query Parquet with partition filtering
-- Only reads files from the 2019 partition
SELECT *
FROM OPENROWSET(
    BULK 'sales/parquet/year=*/*.snappy.parquet',
    DATA_SOURCE = 'sales_data',
    FORMAT = 'PARQUET'
) AS orders
WHERE orders.filepath(1) = '2019';  -- Filter to only read 2019 partition


-------------------------------------------------------------------------------
-- SECTION 5: EXTERNAL TABLES (Virtual Tables over Data Lake Files)
-------------------------------------------------------------------------------

-- Create an External Table to query files like a regular table
-- External tables = metadata layer over files, no data movement
CREATE EXTERNAL TABLE dbo.orders (
    SalesOrderNumber VARCHAR(10),
    SalesOrderLineNumber INT,
    OrderDate DATE,
    CustomerName VARCHAR(25),
    EmailAddress VARCHAR(50),
    Item VARCHAR(30),
    Quantity INT,
    UnitPrice DECIMAL(18,2),
    TaxAmount DECIMAL(18,2)
) WITH (
    DATA_SOURCE = sales_data,  -- Points to the ADLS Gen2 location
    LOCATION = 'sales/csv/*.csv',  -- Path to the CSV files
    FILE_FORMAT = CsvFormat  -- Uses the CSV format defined earlier
);
GO;

-- Now you can query the external table like a regular table
-- Much simpler syntax than OPENROWSET
SELECT 
    YEAR(OrderDate) AS OrderYear,
    SUM((UnitPrice * Quantity) + TaxAmount) AS GrossRevenue
FROM dbo.orders
GROUP BY YEAR(OrderDate)
ORDER BY OrderYear;


-------------------------------------------------------------------------------
-- SECTION 6: CTAS (CREATE TABLE AS SELECT) - Transform and Persist Results
-------------------------------------------------------------------------------

-- Create an External Table using CTAS (Create Table As Select)
-- CTAS combines table creation and data insertion in one statement
-- Results are written back to Azure Data Lake as Parquet files
CREATE EXTERNAL TABLE ProductSalesTotals WITH (
    LOCATION = 'sales/productsales/',  -- Subdirectory inside ADLS Gen2 where Parquet files will be stored (DESTINATION)
    DATA_SOURCE = sales_data,  -- References the external storage location (ADLS Gen2)
    FILE_FORMAT = ParquetFormat  -- Specifies the file format used to store data
) AS
SELECT 
    Item AS Product,  -- Renames 'Item' column to 'Product'
    SUM(Quantity) AS ItemsSold,  -- Calculates the total quantity of items sold
    ROUND(SUM(UnitPrice) - SUM(TaxAmount), 2) AS NetRevenue  -- Computes net revenue (excluding tax), rounded to 2 decimal places
FROM OPENROWSET(
    BULK 'sales/csv/*.csv',  -- Reads all CSV files from the specified folder (SOURCE)
    DATA_SOURCE = 'sales_data',  -- References the external data source
    FORMAT = 'CSV',  -- Specifies that the source files are in CSV format
    PARSER_VERSION = '2.0',  -- Uses CSV parser version 2.0 for improved parsing
    HEADER_ROW = TRUE  -- Indicates that the first row contains column headers
) AS orders
GROUP BY Item;  -- Groups results by product to calculate aggregated values
GO;

/*
KEY CONCEPT - CTAS Path Roles:
    LOCATION (in WITH clause) --> Path for the Result (Destination) folder where new files are written
    BULK (in OPENROWSET)      --> Path for the Source files being read
    
    Example:
    - Source: sales/csv/*.csv           (reads existing CSV files)
    - Destination: sales/productsales/  (writes new Parquet files)
*/*/

/*
-------------------------------------------------------------------------------
-- SECTION 7: STORED PROCEDURES WITH CTAS FOR AUTOMATION
-------------------------------------------------------------------------------

-- Objective: Create a stored procedure to refresh YearlySales data in an external table
-- Steps:
-- 1. Create external resources (data source and file format) if not already created
-- 2. Create the CTAS query to aggregate yearly sales
-- 3. Encapsulate the CTAS inside a stored procedure for reusability
-- 4. Add logic to drop and recreate the table each time (refresh pattern)
*/
USE Sales;
GO;

-- Step 1: Create external data source for yearly sales
-- This defines both the Source path (for reading) and Result destination (for writing)
CREATE EXTERNAL DATA SOURCE YearlySales_DS WITH (
    LOCATION = 'https://datalake6tbnq4u.dfs.core.windows.net/files/sales/'
);
GO;

-- Step 2: Create the file format for the result (Parquet with Snappy compression)
CREATE EXTERNAL FILE FORMAT YearlySales_Format WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);
GO;

-- Step 3 & 4: Create stored procedure that encapsulates the CTAS logic
CREATE OR ALTER PROC YearlySales_proc AS
BEGIN
    -- Drop existing table if it exists (refresh pattern)
    -- This ensures we always have fresh data when the procedure runs
    IF EXISTS (
        SELECT * FROM sys.external_tables
        WHERE name = 'YearlySales'
    )
    DROP EXTERNAL TABLE YearlySales;
    
    -- Create external table with aggregated yearly sales data
    CREATE EXTERNAL TABLE YearlySales WITH (
        LOCATION = 'YearlySales_Folder',  -- Destination folder for Parquet files
        DATA_SOURCE = YearlySales_DS,  -- Points to ADLS Gen2 location
        FILE_FORMAT = YearlySales_Format  -- Uses Parquet format with Snappy compression
    ) AS
    -- The SELECT statement that performs the aggregation
    SELECT 
        YEAR(OrderDate) AS CalendarYear,
        SUM(Quantity) AS ItemsSold,
        ROUND(SUM(UnitPrice) - SUM(TaxAmount), 2) AS NetRevenue
    FROM OPENROWSET(
        BULK 'csv/*.csv',  -- Source: Read CSV files (relative path from data source)
        DATA_SOURCE = 'YearlySales_DS',
        FORMAT = 'CSV',
        HEADER_ROW = TRUE,
        PARSER_VERSION = '2.0'
    ) AS rows
    GROUP BY YEAR(OrderDate);  -- Group by year to get yearly totals
END;  -- End of the stored procedure
GO;

-- Execute the stored procedure to create/refresh the YearlySales table
EXEC YearlySales_proc;
GO;

-- Query the created external table
SELECT * FROM YearlySales
ORDER BY CalendarYear;


-------------------------------------------------------------------------------
-- SECTION 8: DEDICATED SQL POOL - DIMENSIONAL MODEL QUERIES
-------------------------------------------------------------------------------

-- Query data warehouse tables using dimensional model (star schema)
-- Fact table (FactResellerSales) joined with dimension tables (DimDate, DimEmployee, etc.)

-- Sales by fiscal year and quarter
SELECT 
    d.FiscalYear AS FY,
    d.FiscalQuarter AS FQ,
    SUM(r.OrderQuantity) AS ItemsSold
FROM FactResellerSales AS r
JOIN DimDate AS d ON r.OrderDateKey = d.DateKey  -- Join on date key (surrogate key)
GROUP BY d.FiscalYear, d.FiscalQuarter
ORDER BY FY, FQ;

-- Sales by fiscal period and sales territory
-- Multiple joins to navigate through the dimensional model
SELECT 
    d.FiscalYear AS FY,
    d.FiscalQuarter AS FQ,
    t.SalesTerritoryRegion AS SalesTerritory,
    SUM(r.OrderQuantity) AS ItemsSold
FROM FactResellerSales AS r
JOIN DimDate AS d ON r.OrderDateKey = d.DateKey
JOIN DimEmployee AS e ON r.EmployeeKey = e.EmployeeKey  -- Join to get employee info
JOIN DimSalesTerritory AS t ON e.SalesTerritoryKey = t.SalesTerritoryKey  -- Join to get territory
GROUP BY d.FiscalYear, d.FiscalQuarter, t.SalesTerritoryRegion
ORDER BY FY, FQ, SalesTerritory;

-- Sales by fiscal period, territory, and product category
-- Even more joins to get product category information
SELECT 
    d.FiscalYear AS FY,
    d.FiscalQuarter AS FQ,
    t.SalesTerritoryRegion AS SalesTerritory,
    pc.EnglishProductCategoryName AS ProductCategory,
    SUM(r.OrderQuantity) AS ItemsSold
FROM FactResellerSales AS r
JOIN DimDate AS d ON r.OrderDateKey = d.DateKey
JOIN DimEmployee AS e ON r.EmployeeKey = e.EmployeeKey
JOIN DimSalesTerritory AS t ON e.SalesTerritoryKey = t.SalesTerritoryKey
JOIN DimProduct AS p ON r.ProductKey = p.ProductKey  -- Join to product
JOIN DimProductSubcategory AS ps ON p.ProductSubcategoryKey = ps.ProductSubcategoryKey  -- Navigate to subcategory
JOIN DimProductCategory AS pc ON ps.ProductCategoryKey = pc.ProductCategoryKey  -- Navigate to category
GROUP BY d.FiscalYear, d.FiscalQuarter, t.SalesTerritoryRegion, pc.EnglishProductCategoryName
ORDER BY FY, FQ, SalesTerritory, ProductCategory;


-------------------------------------------------------------------------------
-- SECTION 9: WINDOW FUNCTIONS - Advanced Analytics
-------------------------------------------------------------------------------

-- Window functions allow calculations across rows without grouping
-- OVER clause defines the "window" of rows to include in the calculation

-- Ranked sales territories by year using RANK() window function
SELECT 
    d.FiscalYear,
    t.SalesTerritoryRegion AS SalesTerritory,
    SUM(s.SalesAmount) AS TerritoryTotal,
    -- Window function: SUM all territory totals for each year
    SUM(SUM(s.SalesAmount)) OVER(PARTITION BY d.FiscalYear) AS YearTotal,
    -- Window function: RANK territories within each year by sales amount
    RANK() OVER(
        PARTITION BY d.FiscalYear  -- Separate ranking for each year
        ORDER BY SUM(s.SalesAmount) DESC  -- Rank by sales amount (highest first)
    ) AS RankForYear,
    -- Calculate percentage of yearly total for each territory
    CAST(
        ROUND(
            (SUM(s.SalesAmount) / SUM(SUM(s.SalesAmount)) OVER(PARTITION BY d.FiscalYear)) * 100,
            2
        ) AS DECIMAL(5,2)
    ) AS PercentOfYear
FROM FactResellerSales AS s
JOIN DimDate AS d ON s.OrderDateKey = d.DateKey
JOIN DimEmployee AS e ON s.EmployeeKey = e.EmployeeKey
JOIN DimSalesTerritory AS t ON e.SalesTerritoryKey = t.SalesTerritoryKey
GROUP BY d.FiscalYear, t.SalesTerritoryRegion
ORDER BY d.FiscalYear, RankForYear;

-- APPROX_COUNT_DISTINCT for performance optimization
-- For large datasets, approximate count is much faster than exact count
SELECT 
    d.FiscalYear,
    t.SalesTerritoryRegion AS SalesTerritory,
    APPROX_COUNT_DISTINCT(s.SalesOrderNumber) AS ApproxOrders,  -- Faster, approximate count
    COUNT(DISTINCT s.SalesOrderNumber) AS ExactOrders  -- Slower, exact count
FROM FactResellerSales AS s
JOIN DimDate AS d ON s.OrderDateKey = d.DateKey
JOIN DimEmployee AS e ON s.EmployeeKey = e.EmployeeKey
JOIN DimSalesTerritory AS t ON e.SalesTerritoryKey = t.SalesTerritoryKey
GROUP BY d.FiscalYear, t.SalesTerritoryRegion
ORDER BY d.FiscalYear, ApproxOrders DESC;


-------------------------------------------------------------------------------
-- SECTION 10: PRODUCT SALES ANALYSIS
-------------------------------------------------------------------------------

-- Monthly product sales aggregation across multiple dimension tables
SELECT 
    d.CalendarYear,
    d.MonthNumberOfYear,
    d.EnglishMonthName AS Month,
    p.EnglishProductName AS Product,
    SUM(o.OrderQuantity) AS UnitsSold,
    SUM(o.SalesAmount) AS Revenue
FROM dbo.FactInternetSales AS o
JOIN dbo.DimDate AS d ON o.OrderDateKey = d.DateKey
JOIN dbo.DimProduct AS p ON o.ProductKey = p.ProductKey
GROUP BY 
    d.CalendarYear, 
    d.MonthNumberOfYear, 
    d.EnglishMonthName, 
    p.EnglishProductName
ORDER BY d.CalendarYear, d.MonthNumberOfYear, Revenue DESC;

/*
/*******************************************************************************
 * END OF TUTORIAL SQL PORTFOLIO
 * 
 * Key Concepts Covered:
 * ✓ OPENROWSET - Query files without loading
 * ✓ External Data Sources - Centralize storage locations
 * ✓ External File Formats - Define reusable format specs
 * ✓ External Tables - Create metadata layer over files
 * ✓ CTAS - Transform and persist query results
 * ✓ Stored Procedures - Automate recurring operations
 * ✓ Partition Pruning - Optimize query performance
 * ✓ Window Functions - Advanced analytics without grouping
 * ✓ Dimensional Modeling - Star schema queries
 *
 * Remember:
 * - Serverless pools = Compute only, no data storage
 * - External tables = Metadata only, data stays in Data Lake
 * - CTAS = Writes results back to Data Lake as new files
 * - Partition pruning = Only read relevant file partitions
 * - Window functions = Calculations across rows without GROUP BY
 ******************************************************************************/
 */