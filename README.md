# End-to-End Banking Data Engineering Pipeline with PySpark and Databricks

An end-to-end banking data engineering project that processes customer, account, and transaction data using **PySpark and Databricks**. The project demonstrates incremental data ingestion, data transformation, SCD Type 2 implementation, business KPI generation, and workflow orchestration using a Bronze-Silver-Gold architecture.

---

##  Project Overview

This project implements a scalable banking data pipeline that processes customer, account, and transaction data through multiple layers.

The pipeline follows a **Bronze → Silver → Gold** architecture:

```text
CSV Files
    ↓
Landing Layer
    ↓
Auto Loader
    ↓
Bronze Layer
    ↓
Silver Layer
    ↓
Gold Layer
    ↓
Business Views
    ↓
Analytics / BI
```

The pipeline is orchestrated using **Databricks Workflows**, with individual tasks for ingestion, transformation, KPI generation, and view creation.

---

##  Objectives

- Implement incremental file ingestion using Databricks Auto Loader
- Process banking customer, account, and transaction data
- Maintain separate checkpoints for each data source
- Apply explicit schemas during ingestion
- Clean and transform data using PySpark
- Implement Slowly Changing Dimension Type 2 (SCD Type 2) for customer history
- Generate business-level banking KPIs
- Create SQL views for downstream analytics
- Orchestrate the complete pipeline using Databricks Workflows

---

##  Architecture

```text
                         ┌─────────────────────┐
                         │     Landing CSV     │
                         │                     │
                         │ Customers           │
                         │ Accounts            │
                         │ Transactions        │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │    Auto Loader      │
                         │ Incremental Ingest  │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │    Bronze Layer     │
                         │       Delta         │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │    Silver Layer     │
                         │                     │
                         │ Cleaning            │
                         │ Transformation     │
                         │ SCD Type 2          │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │     Gold Layer      │
                         │                     │
                         │ Customer KPIs       │
                         │ Account KPIs        │
                         │ Transaction KPIs    │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │   Business Views    │
                         └──────────┬──────────┘
                                    │
                                    ▼
                              BI / Analytics
```

---

##  Data Sources

The project uses three primary datasets.

### Customers

Contains customer master and historical information.

| Column | Description |
|---|---|
| customer_id | Unique customer identifier |
| name | Customer name |
| dob | Date of birth |
| kyc_status | KYC verification status |
| address | Customer address |
| updated_at | Record update timestamp/date |

### Accounts

Contains customer account information.

| Column | Description |
|---|---|
| account_id | Unique account identifier |
| customer_id | Associated customer |
| account_type | Savings / Current |
| balance | Account balance |

### Transactions

Contains banking transaction records.

| Column | Description |
|---|---|
| txn_id | Unique transaction identifier |
| account_id | Associated account |
| amount | Transaction amount |
| txn_type | Credit / Debit |
| txn_date | Transaction date |

---

#  Incremental Ingestion

Databricks **Auto Loader** is used to incrementally ingest new CSV files from the landing layer.

Example:

```text
landing/
├── customers/
│   ├── customers_batch_001.csv
│   └── customers_batch_002.csv
│
├── accounts/
│   ├── accounts_batch_001.csv
│   └── accounts_batch_002.csv
│
└── transactions/
    ├── transactions_batch_001.csv
    └── transactions_batch_002.csv
```

Auto Loader tracks processed files using checkpoints.

When a new batch is added:

```text
batch_001
    ↓
already processed

batch_002
    ↓
Auto Loader detects new file
    ↓
Bronze append
```

This allows the pipeline to process new data without reprocessing previously ingested files.

---

#  Bronze Layer

The Bronze layer stores the incrementally ingested data as **Delta tables**.

```text
bronze/
├── customers
├── accounts
└── transactions
```

Each ingestion process maintains its own checkpoint:

```text
checkpoints/
├── customers/
├── accounts/
└── transactions/
```

Explicit schemas are used during ingestion to avoid unreliable automatic type inference for important banking fields.

---

#  Silver Layer

The Silver layer performs data cleaning and transformation.

Typical operations include:

- Data type conversion
- Date conversion
- Data cleaning
- Null handling
- Deduplication
- Business transformations
- Customer history management

### Customer SCD Type 2

Customer changes are maintained using **Slowly Changing Dimension Type 2**.

For example:

```text
Customer 1

2024-01-01 → Pune → VERIFIED
2024-06-01 → Mumbai → VERIFIED
2025-01-10 → Mumbai → EXPIRED
```

The SCD Type 2 process maintains historical versions while identifying the current record.

---

#  Gold Layer

The Gold layer contains business-level KPI datasets used for analytics.

### Customer KPIs

Examples include:

- Total transactions per customer
- Total transaction value
- Customer-level transaction activity

### Account KPIs

Examples include:

- Transaction count per account
- Total transaction value
- Average transaction amount
- Account-level transaction activity

### Monthly Transaction KPIs

Examples include:

- Monthly transaction volume
- Total transaction value
- Average transaction amount

---

#  Business Views

SQL views are created on top of the Gold datasets for downstream consumption.

Example:

```sql
CREATE OR REPLACE VIEW bankingprojectpyspark_cata.man_schema.vw_customer_kpis AS
SELECT *
FROM bankingprojectpyspark_cata.man_schema.customer_kpis;
```

The views provide a business-facing layer without creating unnecessary copies of the Gold data.

Architecture:

```text
Gold Delta Tables
       ↓
Business Views
       ↓
Analytics / BI
```

---

#  Workflow Orchestration

The complete pipeline is orchestrated using a **Databricks Workflow** named:

**Banking Data Engineering Workflow**

The workflow manages dependencies between ingestion, transformation, KPI, and view tasks.

Example workflow:

```text
Customer Ingestion
       ↓
Customer Enrichment
       ↓
SCD Type 2

Account Ingestion
       ↓
Account Enrichment

Transaction Ingestion
       ↓
Transaction Enrichment
       ↓
 ┌─────┼──────────────┐
 ↓     ↓              ↓
Account  Customer   Monthly
KPIs     KPIs       Transaction KPIs
 └─────┬──────────────┘
       ↓
   Business Views
```

---

#  Technologies Used

- **Python**
- **PySpark**
- **Apache Spark**
- **Databricks**
- **Databricks Auto Loader**
- **Delta Lake**
- **Databricks Workflows**
- **SQL**

---

#  Project Structure

```text
banking-data-engineering/
│
├── src/
│   │
│   ├── bronze/
│   │   ├── ingest_customers.py
│   │   ├── ingest_accounts.py
│   │   └── ingest_transactions.py
│   │
│   ├── silver/
│   │   ├── enr_customers.py
│   │   ├── enr_accounts.py
│   │   ├── enr_transactions.py
│   │   └── scd2_customers.py
│   │
│   └── gold/
│       ├── customer_kpis.py
│       ├── account_kpis.py
│       └── monthly_transaction_kpis.py
│
├── sql/
│   └── views.sql
│
└── README.md
```

---

#  Key Features

### Incremental Processing

New CSV files are automatically detected and appended to Bronze using Auto Loader.

### Explicit Schema

Predefined schemas are used instead of relying entirely on automatic schema inference.

### SCD Type 2

Historical customer changes are preserved while maintaining the current version.

### Medallion Architecture

The pipeline separates ingestion, transformation, and business-level data.

### Workflow Orchestration

Databricks Workflows manages task execution and dependencies.

### Business Views

Gold datasets are exposed through SQL views for analytics and BI consumption.

---

#  Pipeline Flow

The complete pipeline can be summarized as:

```text
New CSV File
     ↓
Auto Loader
     ↓
Bronze Delta
     ↓
Silver Transformation
     ↓
SCD Type 2 / Data Cleaning
     ↓
Gold KPI Tables
     ↓
Business Views
     ↓
Analytics
```

---

#  Project Outcome

This project demonstrates an end-to-end data engineering workflow for banking data, covering **incremental ingestion, distributed data processing, data transformation, historical data management, business aggregation, and workflow orchestration** using PySpark and Databricks.
