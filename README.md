# 🎫 Operations Ticket Analytics Pipeline

A robust data engineering pipeline that processes operations support ticket data, ingests it into Google Cloud Platform (GCP), and transforms it into analytics-ready tables for business insights.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Data Flow](#data-flow)
- [Setup & Prerequisites](#setup--prerequisites)
- [Pipeline Components](#pipeline-components)
  - [1. Data Ingestion](#1-data-ingestion-ingest_to_gcspy)
  - [2. GCS to BigQuery Backfill](#2-gcs-to-bigquery-backfill-gcs_to_bq_backfillpy)
  - [3. Transform Job](#3-transform-job-transform_jobpy)
- [Output Tables](#output-tables)
- [Configuration](#configuration)
- [Running the Pipeline](#running-the-pipeline)
- [Performance Optimizations](#performance-optimizations)
- [License](#license)

---

## Overview

This project implements an end-to-end data pipeline for analyzing operations support tickets. The pipeline handles:

- **~95MB+ of ticket data** (~300k+ records)
- **Daily partitioning** for efficient storage and querying
- **Agent performance metrics** and workload analysis
- **SLA compliance tracking** and breach rate calculations
- **Backlog monitoring** and trend analysis

---

## Architecture

```
┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│  Source Data     │    │  Google Cloud    │    │    BigQuery      │
│  (CSV Files)     │───▶│  Storage (GCS)   │───▶│   Data Warehouse │
│                  │    │                  │    │                  │
│ • ops_tickets    │    │ • Daily partiti- │    │ • Raw tables     │
│ • agents_master  │    │   oned files     │    │ • Analytics      │
└──────────────────┘    └──────────────────┘    └──────────────────┘
         │                                              │
         │              ┌──────────────────┐            │
         └─────────────▶│   Apache Spark   │◀───────────┘
                        │   (Local Mode)   │
                        │                  │
                        │ • Transformations│
                        │ • Aggregations   │
                        │ • Analytics      │
                        └──────────────────┘
```

---

## Project Structure

```
tickets-de-proj/
│
├── 📁 data/                          # Source data (gitignored)
│   ├── ops_tickets.csv              # Main ticket data (~95MB)
│   └── agents_master.csv            # Agent reference data
│
├── 📁 scripts/                       # Ingestion scripts
│   └── ingest_to_gcs.py             # Upload data to GCS with daily partitions
│
├── 📁 spark_jobs/                    # PySpark transformation jobs
│   ├── gcs_to_bq_backfill.py        # Load raw data from GCS to BigQuery
│   └── transform_job.py             # Create analytics tables
│
├── 📁 spark_jars/                    # Required JAR dependencies (gitignored)
│   ├── gcs-connector.jar            # GCS connector for Hadoop
│   ├── gcs-connector-hadoop2-latest.jar
│   ├── gcs-connector-hadoop3-latest.jar
│   └── spark-bigquery.jar           # BigQuery connector for Spark
│
├── 📁 .venv/                         # Python virtual environment (gitignored)
│
├── .gitignore                        # Git ignore rules
└── README.md                         # This file
```

---

## Data Flow

### Stage 1: Ingestion (CSV → GCS)
```
ops_tickets.csv ──▶ Daily Partitioned Files ──▶ gs://ops-tickets-files/raw/YYYY/MM/DD.csv
agents_master.csv ──▶ gs://ops-tickets-files/agents_data/agents_master.csv
```

### Stage 2: Raw Loading (GCS → BigQuery)
```
gs://ops-tickets-files/raw/**/*.csv ──▶ biz-ops-031099.ops_analytics.tickets_raw
```

### Stage 3: Transformation (BigQuery → Analytics Tables)
```
tickets_raw ──▶ fact_tickets
            ──▶ features_daily
            ──▶ backlog_daily
            ──▶ daily_agent_load
            ──▶ ticket_sla_risk
```

---

## Setup & Prerequisites

### 1. Prerequisites

- **Python 3.8+**
- **Apache Spark 3.x** (local mode)
- **Google Cloud SDK** (`gcloud`)
- **GCP Project** with BigQuery and Cloud Storage enabled

### 2. Python Environment Setup

```powershell
# Create virtual environment
python -m venv .venv

# Activate (PowerShell)
.\.venv\Scripts\Activate.ps1

# Install dependencies
pip install google-cloud-storage google-cloud-bigquery pandas pyspark
```

### 3. GCP Authentication

```powershell
# Login to GCP
gcloud auth application-default login

# Set project
gcloud config set project biz-ops-031099
```

### 4. Required JAR Files

Download and place in `spark_jars/`:

| JAR File | Purpose | Download Link |
|----------|---------|---------------|
| `gcs-connector.jar` | Read/write to GCS from Spark | [GCS Connector](https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage) |
| `spark-bigquery.jar` | Read/write to BigQuery from Spark | [Spark BigQuery](https://github.com/GoogleCloudDataproc/spark-bigquery-connector) |

### 5. GCP Resources Required

| Resource | Name | Purpose |
|----------|------|---------|
| GCS Bucket | `ops-tickets-files` | Raw data storage |
| GCS Bucket | `temp-gcs-bucket-spark01` | Spark temp storage for BigQuery writes |
| BigQuery Dataset | `ops_analytics` | Analytics tables |

---

## Pipeline Components

### 1. Data Ingestion (`ingest_to_gcs.py`)

**Location:** `scripts/ingest_to_gcs.py`

**Purpose:** Reads source CSV files and uploads them to GCS with daily partitioning.

**Features:**
- ✅ Parallel uploads using `ThreadPoolExecutor` (10 workers)
- ✅ Daily partitioning by `created_at` timestamp
- ✅ Memory-optimized batch processing
- ✅ Handles all 12 months of data

**Output Structure:**
```
gs://ops-tickets-files/
├── raw/
│   └── 2023/
│       ├── 01/
│       │   ├── 2023-01-01.csv
│       │   ├── 2023-01-02.csv
│       │   └── ...
│       ├── 02/
│       └── ...
└── agents_data/
    └── agents_master.csv
```

**Run:**
```powershell
python scripts/ingest_to_gcs.py
```

---

### 2. GCS to BigQuery Backfill (`gcs_to_bq_backfill.py`)

**Location:** `spark_jobs/gcs_to_bq_backfill.py`

**Purpose:** Loads raw partitioned data from GCS into BigQuery using PySpark.

**Features:**
- ✅ Month-by-month loading to prevent OOM errors
- ✅ Automatic schema inference
- ✅ Metadata columns (`_ingestion_ts`, `_source_file`)
- ✅ Adaptive query optimization
- ✅ Configurable memory allocation (4GB driver)

**Output Table:** `biz-ops-031099.ops_analytics.tickets_raw`

**Run:**
```powershell
spark-submit spark_jobs/gcs_to_bq_backfill.py
```

---

### 3. Transform Job (`transform_job.py`)

**Location:** `spark_jobs/transform_job.py`

**Purpose:** Reads raw data from BigQuery, applies transformations, and creates analytics tables.

**Features:**
- ✅ Single-pass aggregation for daily metrics
- ✅ DataFrame caching for performance
- ✅ Parallel writes to BigQuery (4 workers)
- ✅ Adaptive Spark SQL optimizations

**Transformations Applied:**

| Transformation | Description |
|----------------|-------------|
| Date Extraction | `created_date`, `resolved_date` from timestamps |
| Resolution Status | `is_resolved` boolean flag |
| Clean Metrics | `resolution_hours_clean` (null for unresolved) |
| SLA Risk Analysis | `is_high_priority`, `is_long_resolution` flags |
| Time Features | `day_of_week`, `hour_of_day` extraction |

**Run:**
```powershell
spark-submit spark_jobs/transform_job.py
```

---

## Output Tables

### `fact_tickets`
Core ticket-level fact table with cleaned and enriched fields.

| Column | Type | Description |
|--------|------|-------------|
| `ticket_id` | STRING | Unique ticket identifier |
| `created_at` | TIMESTAMP | Ticket creation time |
| `resolved_at` | TIMESTAMP | Ticket resolution time |
| `created_date` | DATE | Date portion of created_at |
| `resolved_date` | DATE | Date portion of resolved_at |
| `is_resolved` | BOOLEAN | Resolution status flag |
| `resolution_hours_clean` | FLOAT | Hours to resolve (null if open) |
| `sla_hours` | FLOAT | SLA target hours |
| `sla_breached` | BOOLEAN | SLA breach flag |
| `priority` | STRING | Ticket priority level |
| `category` | STRING | Ticket category |
| `channel` | STRING | Ticket source channel |
| `customer_tier` | STRING | Customer tier |
| `status` | STRING | Current ticket status |
| `agent_id` | STRING | Assigned agent ID |

---

### `features_daily`
Daily aggregate metrics for trend analysis.

| Column | Type | Description |
|--------|------|-------------|
| `ops_date` | DATE | Calendar date |
| `total_tickets` | INT | Total tickets created |
| `resolved_tickets` | INT | Tickets resolved that day |
| `open_tickets` | INT | Tickets still open |
| `avg_tat` | FLOAT | Average resolution time (hours) |
| `sla_breach_rate` | FLOAT | Percentage of SLA breaches |

---

### `backlog_daily`
Daily backlog tracking.

| Column | Type | Description |
|--------|------|-------------|
| `ops_date` | DATE | Calendar date |
| `backlog_size` | INT | Number of unresolved tickets |

---

### `daily_agent_load`
Agent workload and performance metrics.

| Column | Type | Description |
|--------|------|-------------|
| `ops_date` | DATE | Calendar date |
| `agent_id` | STRING | Agent identifier |
| `tickets_assigned` | INT | Tickets assigned that day |
| `tickets_resolved` | INT | Tickets resolved that day |
| `avg_resolution_hours` | FLOAT | Agent's avg resolution time |
| `sla_breaches` | INT | Count of SLA breaches |
| `open_tickets_from_day` | INT | Tickets left open from assignments |

---

### `ticket_sla_risk`
Ticket-level SLA risk analysis with time features.

| Column | Type | Description |
|--------|------|-------------|
| `ticket_id` | STRING | Unique ticket identifier |
| `created_at_ts` | TIMESTAMP | Creation timestamp |
| `created_date` | DATE | Creation date |
| `priority` | STRING | Ticket priority |
| `category` | STRING | Ticket category |
| `agent_id` | STRING | Assigned agent |
| `resolution_hours_clean` | FLOAT | Actual resolution hours |
| `sla_hours` | FLOAT | SLA target hours |
| `sla_breached` | BOOLEAN | SLA breach flag |
| `day_of_week` | STRING | Day name (Mon, Tue, etc.) |
| `hour_of_day` | INT | Hour of creation (0-23) |
| `is_high_priority` | BOOLEAN | Critical or High priority flag |
| `is_long_resolution` | BOOLEAN | Resolution exceeded SLA flag |

---

## Configuration

### Key Configuration Variables

| Variable | Location | Value | Description |
|----------|----------|-------|-------------|
| `PROJECT_ID` | `transform_job.py` | `biz-ops-031099` | GCP Project ID |
| `DATASET_ID` | `transform_job.py` | `ops_analytics` | BigQuery dataset |
| `bucket_name` | `ingest_to_gcs.py` | `ops-tickets-files` | GCS bucket for raw data |
| `TEMP_GCS_BUCKET` | `transform_job.py` | `temp-gcs-bucket-spark01` | Temp bucket for Spark |
| `YEAR` | `gcs_to_bq_backfill.py` | `2023` | Data year to process |

### Spark Configuration

```python
# Memory settings
.config("spark.driver.memory", "4g")

# Adaptive optimization
.config("spark.sql.adaptive.enabled", "true")
.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
.config("spark.sql.shuffle.partitions", "200")
```

---

## Running the Pipeline

### Full Pipeline Execution Order

```powershell
# Step 1: Activate environment
.\.venv\Scripts\Activate.ps1

# Step 2: Ingest data to GCS (one-time setup)
python scripts/ingest_to_gcs.py

# Step 3: Backfill raw data to BigQuery
spark-submit spark_jobs/gcs_to_bq_backfill.py

# Step 4: Transform and create analytics tables
spark-submit spark_jobs/transform_job.py
```

### For Daily/Incremental Runs

Modify the scripts to process specific date ranges instead of full backfill.

---

## Performance Optimizations

### 1. **Parallel I/O**
- `ThreadPoolExecutor` for concurrent GCS uploads (10 workers)
- Parallel BigQuery writes (4 workers)

### 2. **Memory Management**
- Month-by-month processing prevents OOM
- `spark.catalog.clearCache()` after each batch
- DataFrame `unpersist()` after use

### 3. **Spark Optimizations**
- Adaptive Query Execution (AQE) enabled
- Dynamic partition coalescing
- Cached intermediate DataFrames

### 4. **Single-Pass Aggregation**
- Combined daily metrics calculation instead of multiple groupBy operations

---

## Source Data Schema

### `ops_tickets.csv`

| Column | Type | Description |
|--------|------|-------------|
| `ticket_id` | STRING | Unique identifier |
| `created_at` | TIMESTAMP | Creation timestamp |
| `resolved_at` | STRING/TIMESTAMP | Resolution timestamp |
| `resolution_hours` | FLOAT | Hours to resolution |
| `sla_hours` | FLOAT | SLA target |
| `sla_breached` | BOOLEAN | Breach indicator |
| `priority` | STRING | Priority level |
| `category` | STRING | Ticket category |
| `channel` | STRING | Source channel |
| `customer_tier` | STRING | Customer tier |
| `status` | STRING | Current status |
| `agent_id` | STRING | Assigned agent |

### `agents_master.csv`

Contains agent reference data including agent IDs and metadata.

---

## Tech Stack

| Technology | Version | Purpose |
|------------|---------|---------|
| Python | 3.8+ | Scripting and orchestration |
| Apache Spark | 3.x | Distributed data processing |
| Google Cloud Storage | - | Raw data lake |
| BigQuery | - | Data warehouse |
| Pandas | - | Local data manipulation |

---

## Future Enhancements

- [ ] Add Airflow/Cloud Composer for orchestration
- [ ] Implement incremental loading
- [ ] Add data quality checks with Great Expectations
- [ ] Create Looker/Data Studio dashboards
- [ ] Add unit tests for transformations
- [ ] Containerize with Docker

---

## License

This project is for internal data engineering purposes.

---

## Author

Built with ❤️ for operations analytics.
