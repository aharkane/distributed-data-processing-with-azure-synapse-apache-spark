# Azure Synapse Analytics Data Engineering

PySpark and serverless SQL project on Azure Synapse with Delta Lake implementation.

## Overview

Cloud data engineering implementation demonstrating Azure Synapse capabilities for distributed data processing. Includes data ingestion, PySpark transformations with schema enforcement, Delta Lake with ACID transactions, and serverless SQL analytics.

## Architecture

```
Azure Data Lake Storage Gen2
    ↓
Apache Spark Pool (PySpark)
    ├── Schema Enforcement
    ├── Transformations
    └── Delta Lake (ACID)
        ↓
Serverless SQL Pool (OPENROWSET)
```

## Key Features

### Delta Lake Implementation
- ACID transactions for data consistency
- Schema enforcement at write time
- Time travel for historical queries
- Transaction log audit trail
- Update/delete operations on data lake

### PySpark Processing
- StructType schema definitions
- Year/Month hierarchical partitioning
- Complex transformations (name parsing, aggregations)
- Window functions for analytics
- Multi-format support (CSV, Parquet, JSON, Delta)

### Serverless SQL Analytics
- OPENROWSET for direct Data Lake querying
- External tables over Data Lake files
- Multi-format queries (CSV/Parquet/JSON)
- Pay-per-query consumption model

## Technology Stack

| Component | Technology |
|-----------|------------|
| **Platform** | Azure Synapse Analytics |
| **Processing** | Apache Spark 3.x with PySpark |
| **Storage** | Azure Data Lake Storage Gen2 |
| **Table Format** | Delta Lake |
| **SQL Engine** | Serverless SQL Pool |
| **Formats** | Parquet, CSV, JSON, Delta |

## Results

- Multi-year datasets with partitioning strategies
- Schema-enforced data quality
- ACID-compliant data lake operations
- Cost-effective serverless querying
- Time travel and transaction history

## Skills Demonstrated

<table>
<tr>
<td width="55%" valign="top">

**Cloud Data Engineering**
- Azure Synapse workspace
- Data Lake Storage Gen2
- Spark pool configuration
- Serverless SQL pools

**PySpark & Spark**
- DataFrame API
- Schema enforcement (StructType)
- Partitioning strategies
- Window functions
- Complex transformations

</td>
<td width="45%" valign="top">

**Delta Lake**
- ACID transactions
- Time travel queries
- Transaction history
- Schema evolution
- OPTIMIZE operations

**SQL Analytics**
- OPENROWSET functions
- External table creation
- Multi-format querying
- Serverless optimization

</td>
</tr>
</table>