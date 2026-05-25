# Retail Data Lakehouse: End-to-End Azure Data Engineering Project

An end-to-end cloud data engineering project implementing a modern **Data Lakehouse architecture** using Azure Data Factory, Azure Data Lake Storage (ADLS) Gen2, Databricks, and Unity Catalog. The project ingests raw retail transactional data and processes it through a unified Medallion layers structure, terminating in an optimized Star Schema for business intelligence and analytics.

---

## 🛠️ Tech Stack & Architecture

| Component | Tool / Service |
| :--- | :--- |
| **Orchestration / Ingestion** | Azure Data Factory (ADF) |
| **Storage Layer** | Azure Data Lake Storage (ADLS) Gen2 (Hierarchical namespace enabled) |
| **Compute / Processing Engine** | Apache Spark / PySpark (Databricks) |
| **Data Governance / Security** | Databricks Unity Catalog (3-Level Namespace) |
| **Format** | Delta Lake |
| **Version Control** | GitHub Integration with Databricks Git Folders |

---

## 🏗️ Data Architecture Flow

The pipeline follows the industry-standard **Medallion Architecture**, separating data based on its quality, structure, and readiness for analysis.

```
┌─────────────────────────────────────────┐
│       GitHub Repository (Order.json)    │
└─────────────────────────────────────────┘
                     │
              (HTTP Connector)
                     │
                     ▼
┌─────────────────────────────────────────┐
│         Azure Data Factory (ADF)        │
│                                         │
│  ┌───────────────────────────────────┐  │
│  │     1. Copy Activity              │  │
│  │       - Flatten nested JSON       │  │
│  │       - Sink: Parquet → Bronze    │  │
│  └───────────────────────────────────┘  │
│                    │                    │
│                    ▼                    │
│  ┌───────────────────────────────────┐  │
│  │     2. Managed Identity Auth      │  │
│  │       - Unity Catalog credentials │  │
│  │       - Secure ADLS Gen2 mount    │  │
│  └───────────────────────────────────┘  │
└─────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│              Databricks + Unity Catalog                         │
│                  Medallion Architecture                         │
│                                                                 │
│  ┌─────────────────┐  ──▶  ┌─────────────────┐  ──▶  ┌──────────────────┐  │
│  │   🥉 Bronze      │       │   🥈 Silver      │       │   🥇 Gold         │  │
│  │   Raw Tables    │       │ Cleaned Tables  │       │   Star Schema    │  │
│  │                 │       │                 │       │                  │  │
│  │ - input_file    │       │ - Null purge    │       │ - fact_sales     │  │
│  │   _name()       │       │   on keys       │       │ - dim_customer   │  │
│  │ - ingestion_    │       │ - .trim()       │       │ - dim_product    │  │
│  │   time stamp    │       │   whitespace    │       │ - dim_store      │  │
│  │ - Append-only   │       │ - Type casting  │       │ - dim_date       │  │
│  └─────────────────┘       └─────────────────┘       └──────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### 1. Data Ingestion (Azure Data Factory)

- **Source:** Raw transactional data delivered as nested `JSON` files.
- **Process:** An ADF **Copy Activity** connects securely to the source data stream, flattens/converts the raw formats, and lands them directly into the **Bronze** landing zone.
- **Destination Format:** Optimized `Parquet` storage.

### 2. The Medallion Pipeline (Databricks + Unity Catalog)

Data is managed dynamically within Databricks using a secure Managed Identity connection mapped via **Unity Catalog Storage Credentials**.

#### 🥉 Bronze Layer — Raw Tables
Lands the raw Parquet files as append-only managed Delta Tables. PySpark injects critical metadata at this stage, capturing the `input_file_name()` and `bronze_ingestion_time` for complete lineage tracking and audit logs.

#### 🥈 Silver Layer — Cleaned Tables
Enforces data quality rules. This layer purges invalid null blocks from critical transactional keys (`TransactionID`, `ProductID`), strips leading/trailing whitespaces with `.trim()`, standardizes string casing for categorical grouping, and updates data types to prevent analytical schema drifts.

#### 🥇 Gold Layer — Curation & Star Schema
Aggregates and normalizes the flat Silver records into a high-performance **Star Schema** data warehouse layer.

---

## 📊 Gold Layer Star Schema

To maximize analytical query speeds for downstream dashboard environments (like Power BI), the flat dataset is decoupled into highly performant dimensional categories and supported by a custom calendar table:

| Table Type | Table Name | Key Attributes |
| :--- | :--- | :--- |
| **Fact** | `fact_sales` | TransactionID, Quantities, Currency Amounts, Date Foreign Keys |
| **Dimension** | `dim_customer` | CustomerID, CustomerAge, Gender, CustomerLoyaltyLevel |
| **Dimension** | `dim_product` | ProductID, ProductName, ProductCategory, SubCategory, Brand |
| **Dimension** | `dim_store` | StoreID, StoreLocation, StoreRegion |
| **Dimension** | `dim_date` | Date, Year, Month, Quarter, DayOfWeek, DayName (Time Intelligence) |

---

## 📁 Repository Structure

```text
├── pipeline/                 # Azure Data Factory Pipeline JSON configurations
├── dataset/                  # ADF Input/Output Dataset JSON mappings
├── linkedService/            # Secure infrastructure connection metadata
├── databricks_code/          # Spark/PySpark Development Notebooks
│    ├── 01_Ingestion/        # ADLS Raw Parquet to Unity Catalog Bronze
│    ├── 02_Processing/       # Data Quality cleansing, trimming, and validation
│    └── 03_Curation/         # Analytical Normalization into Star Schema Dimensions/Facts
│         ├── 📓 01_dim_customer    # Isolates customer descriptive data
│         ├── 📓 02_dim_product     # Isolates product attributes
│         ├── 📓 03_dim_store       # Isolates retail location characteristics
│         ├── 📓 04_dim_date        # Generates time-intelligence calendar attributes
│         └── 📓 05_fact_sales      # Central metrics table linked via foreign keys
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites

- Azure Subscription with the following services provisioned:
  - Azure Data Factory
  - Azure Data Lake Storage Gen2
  - Azure Databricks Workspace
- Databricks Unity Catalog enabled
- GitHub account (for Git Folder integration)

### Setup Steps

1. **Clone the repository** into your Databricks workspace via Git Folders.
2. **Deploy ADF resources** by importing the JSON files from `/pipeline/`, `/dataset/`, and `/linkedService/` into your Azure Data Factory instance.
3. **Configure Unity Catalog** storage credentials to point to your ADLS Gen2 container.
4. **Run notebooks in order:**
   - `01_Ingestion` → lands Parquet files into the Bronze Delta table
   - `02_Processing` → applies data quality rules to produce the Silver layer
   - `03_Curation` → builds all five Star Schema tables in the Gold layer
5. **Connect Power BI** (or your preferred BI tool) to the Gold layer via the Databricks SQL Warehouse endpoint.

---

## 📐 Design Decisions

**Why Delta Lake?** Delta provides ACID transactions, schema enforcement, and time-travel capabilities — critical for reliable, auditable data pipelines in a shared lakehouse environment.

**Why Unity Catalog?** Centralizes governance across all Databricks workspaces, enabling fine-grained access controls, data lineage, and a unified 3-level namespace (`catalog.schema.table`).

**Why a Star Schema in Gold?** Dimensional modeling minimizes query complexity for business analysts and BI tools, reducing join overhead and enabling fast aggregations on large retail datasets.

