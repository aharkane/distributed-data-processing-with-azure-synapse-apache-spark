# Distributed Data Processing with Azure Synapse & Apache Spark
---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Project Architecture](#project-architecture)
3. [Featured Code Examples](#featured-code-examples)
   1. [PySpark: Schema Enforcement with Explicit Types](#pyspark-schema-enforcement-with-explicit-types)
   2. [PySpark: Delta Lake with Time Travel](#pyspark-delta-lake-with-time-travel)
   3. [PySpark: Data Transformation & Partitioning](#pyspark-data-transformation--partitioning)
   4. [SQL: OPENROWSET for Serverless Data Exploration](#sql-openrowset-for-serverless-data-exploration)
   5. [SQL: Multi-Format Queries with Partition Pruning](#sql-multi-format-queries-with-partition-pruning)
   6. [SQL: CTAS (Create Table As Select) for Data Persistence](#sql-ctas-create-table-as-select-for-data-persistence)
   7. [SQL: Window Functions for Advanced Analytics](#sql-window-functions-for-advanced-analytics)
   8. [SQL: Dimensional Model Star Schema Query](#sql-dimensional-model-star-schema-query)
   








## Executive Summary

**Overview**  
This project is realized sudring my preparation to the [exam DP-203: Data Engineering on Microsoft Azure Certfication](https://learn.microsoft.com/en-us/users/amineharkane-6987/credentials/19a5370ba9092706). Also to demonstrates my **enterprise-level data engineering** capabilities using Azure Synapse Analytics and Apache Spark. The implementation covers a complete data pipeline including data ingestion, transformation, aggregation, and analytical queries on multi-year sales and customer datasets.       

**Tasks and Goals**
- Design and implement scalable data processing workflows in Azure Synapse
- Demonstrate proficiency with both Spark and SQL for distributed computing
- Build Delta Lake data structures with ACID compliance and time-travel capabilities
- Create dimensional models and analytical queries for business intelligence

**Key Accomplishments**   
✓ Processed multi-year datasets with millions of records  
✓ Implemented Delta Lake with version control and time travel  
✓ Built dimensional models supporting complex analytical queries  
✓ Demonstrated advanced Spark transformations and aggregations  
✓ Designed external tables for cost-effective data exploration  
✓ Optimized query performance through partitioning strategies

**Skills Demonstrated**
<table>
<tr>
<td>

*Data Engineering:*
- Distributed data processing with Apache Spark
- ETL/ELT pipeline design and implementation
- Delta Lake and ACID transactions
- Serverless and dedicated SQL pool design
</td>
<td>
    
*Data Architecture:*
- Dimensional modeling and star schemas
- Complex T-SQL with window functions (RANK, SUM OVER)
- OPENROWSET for external data queries
- Data aggregation and business metrics

</td>
</tr>
</table>


## Project Architecture

**Technology Stack**

| Component | Technology |
|-----------|-----------|
| **Data Platform** | Azure Synapse Analytics |
| **Processing Engine** | Apache Spark with PySpark |
| **Storage** | Azure Data Lake Storage Gen2 |
| **Data Format** | Parquet, CSV, JSON |
| **Analytics** | Serverless SQL Pool, Dedicated SQL Pool |
| **Data Lake Format** | Delta Lake with ACID transactions |


**Core Deliverables:**
- [`Azure-Synapse-Spark-Complete-Portfolio.ipynb`](https://github.com/aharkane/azure-synapse-analytics-sql-pyspark-data-engineering/blob/main/Azure-Synapse-Spark-Notebook.ipynb) - PySpark implementations
- [`Azure-Synapse-SQL-Complete-Portfolio.sql`](https://github.com/aharkane/azure-synapse-analytics-sql-pyspark-data-engineering/blob/main/Azure-Synapse-SQL-Scripts.sql) - SQL analytics and transformations
- [`data/`](https://github.com/aharkane/azure-synapse-analytics-sql-pyspark-data-engineering/tree/main/Data) - Source datasets (2019-2021 sales, customer, product data)

**Repository Structure**

```
├── data/
│   ├── 2019.snappy.parquet
│   ├── 2020.snappy.parquet
│   ├── 2021.snappy.parquet
│   ├── customer.xlsx
│   └── SO*.json
├── Azure-Synapse-Spark-Notebook.ipynb
├── Azure-Synapse-SQL-Scripts.sql
└── README.md
```

  



## Featured Code Examples

### PySpark: Schema Enforcement with Explicit Types

```python
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Define explicit schema for data quality
OrderSchema = StructType([
    StructField("SalesOrderNumber", StringType()),
    StructField("SalesOrderLineNumber", IntegerType()),
    StructField("OrderDate", DateType()),
    StructField("CustomerName", StringType()),
    StructField("Email", StringType()),
    StructField("Item", StringType()),
    StructField("Quantity", IntegerType()),
    StructField("UnitPrice", FloatType()),
    StructField("Tax", FloatType())
])

# Load with schema enforcement
df = spark.read.load('abfss://files@datalake.dfs.core.windows.net/sales/orders.csv',
    format='csv', schema=OrderSchema)

# Revenue analysis by year
df.select(
    year("OrderDate").alias("year"),
    "Quantity", "UnitPrice", "Tax"
).groupBy("year").agg(
    expr("round(sum(Quantity * UnitPrice + Tax), 2)").alias("GrossRevenue")
).orderBy(desc("year")).show()
```

### PySpark: Delta Lake with Time Travel

```python
from delta.tables import *

# Create Delta table
delta_table_path = "delta/products-delta"
df.write.format("delta").save(delta_table_path)

# Update records with ACID guarantees
deltaTable = DeltaTable.forPath(spark, delta_table_path)
deltaTable.update(
    condition="ProductID = 771",
    set={"ListPrice": "ListPrice * 0.9"}
)

# Time travel - access previous version
previous_df = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load(delta_table_path)

# View transaction history
deltaTable.history(10).show(20, False, True)
```

### PySpark: Data Transformation & Partitioning

```python
import pyspark.sql.functions as f

# Transform: Split customer name into first and last
transformed_df = (
    order_details
    .withColumn("FirstName", f.split(f.col("CustomerName"), " ").getItem(0))
    .withColumn("LastName", f.split(f.col("CustomerName"), " ").getItem(1))
    .drop("CustomerName")
)

# Partition by Year and Month for query optimization
dated_df = (
    transformed_df
    .withColumn("Year", f.year(f.col("OrderDate")))
    .withColumn("Month", f.month(f.col("OrderDate")))
)

# Write partitioned data
dated_df.write.partitionBy("Year", "Month") \
    .mode("Overwrite") \
    .parquet("abfss://files@datalake.dfs.core.windows.net/partitioned-data")
```

### SQL: OPENROWSET for Serverless Data Exploration

```sql
-- Query CSV files directly from Data Lake without loading
SELECT TOP 100 *
FROM OPENROWSET(
    BULK 'https://datalake.dfs.core.windows.net/files/product_data/products.csv',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE
) AS result;

-- Aggregate by category without data movement
SELECT TOP 10 
    category,
    COUNT(*) AS productCount
FROM OPENROWSET(
    BULK 'https://datalake.dfs.core.windows.net/files/product_data/products.csv',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE
) AS result
GROUP BY category
ORDER BY productCount DESC;
```

### SQL: Multi-Format Queries with Partition Pruning

```sql
-- Query Parquet with partition filtering
SELECT 
    YEAR(OrderDate) AS OrderYear,
    COUNT(*) AS OrderedItems
FROM OPENROWSET(
    BULK 'https://datalake.dfs.core.windows.net/files/sales/parquet/year=*/*.snappy.parquet',
    FORMAT = 'PARQUET'
) AS result
WHERE result.filepath(1) IN ('2019', '2020')  -- Partition pruning
GROUP BY YEAR(OrderDate)
ORDER BY OrderYear ASC;

-- Query JSON and extract properties
SELECT TOP 100
    JSON_VALUE(doc, '$.SalesOrderNumber') AS OrderNumber,
    JSON_VALUE(doc, '$.CustomerName') AS Customer,
    JSON_VALUE(doc, '$.Item') AS Item,
    JSON_VALUE(doc, '$.Quantity') AS Quantity
FROM OPENROWSET(
    BULK 'https://datalake.dfs.core.windows.net/files/sales/json/*.json',
    FORMAT = 'CSV',
    FIELDTERMINATOR = '0x0b',
    FIELDQUOTE = '0x0b',
    ROWTERMINATOR = '0x0b'
) WITH (doc NVARCHAR(MAX)) AS rows;
```

### SQL: CTAS (Create Table As Select) for Data Persistence

```sql
-- Transform and persist results as external table
CREATE EXTERNAL TABLE ProductSalesTotals
WITH (
    LOCATION = 'sales/product_sales',
    DATA_SOURCE = sales_data,
    FILE_FORMAT = ParquetFormat
)
AS
SELECT 
    Item AS Product,
    SUM(Quantity) AS ItemsSold,
    ROUND(SUM(UnitPrice) - SUM(TaxAmount), 2) AS NetRevenue
FROM OPENROWSET(
    BULK = 'sales/csv/*.csv',
    DATA_SOURCE = sales_data,
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE
) AS orders
GROUP BY Item;
```

### SQL: Window Functions for Advanced Analytics

```sql
-- Ranked sales territories by year with percentage contribution
SELECT 
    d.FiscalYear,
    t.SalesTerritoryRegion AS SalesTerritory,
    SUM(s.SalesAmount) AS TerritoryTotal,
    
    -- Window function: Calculate yearly total
    SUM(SUM(s.SalesAmount)) OVER(PARTITION BY d.FiscalYear) AS YearTotal,
    
    -- Window function: Rank territories within each year
    RANK() OVER(
        PARTITION BY d.FiscalYear 
        ORDER BY SUM(s.SalesAmount) DESC
    ) AS RankForYear,
    
    -- Calculate percentage of yearly total
    CAST(
        ROUND(
            SUM(s.SalesAmount) / 
            SUM(SUM(s.SalesAmount)) OVER(PARTITION BY d.FiscalYear) * 100, 
            2
        ) AS DECIMAL(5,2)
    ) AS PercentOfYear
FROM FactResellerSales AS s
JOIN DimDate AS d ON s.OrderDateKey = d.DateKey
JOIN DimEmployee AS e ON s.EmployeeKey = e.EmployeeKey
JOIN DimSalesTerritory AS t ON e.SalesTerritoryKey = t.SalesTerritoryKey
GROUP BY d.FiscalYear, t.SalesTerritoryRegion
ORDER BY d.FiscalYear, RankForYear;
```

### SQL: Dimensional Model Star Schema Query

```sql
-- Multi-dimensional analysis across fact and dimension tables
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
JOIN DimProduct AS p ON r.ProductKey = p.ProductKey
JOIN DimProductSubcategory AS ps ON p.ProductSubcategoryKey = ps.ProductSubcategoryKey
JOIN DimProductCategory AS pc ON ps.ProductCategoryKey = pc.ProductCategoryKey
GROUP BY d.FiscalYear, d.FiscalQuarter, t.SalesTerritoryRegion, pc.EnglishProductCategoryName
ORDER BY FY, FQ, SalesTerritory, ProductCategory;
```




---
