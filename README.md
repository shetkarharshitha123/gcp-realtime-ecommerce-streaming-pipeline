# Real-Time E-Commerce GCP Streaming Pipeline

A GCP-based e-commerce data pipeline that demonstrates event-driven data ingestion, distributed data processing, operational storage, federated analytics, and data quality validation.

The pipeline ingests e-commerce order events through Google Cloud Pub/Sub, stores raw events in Google Cloud Storage (GCS), processes and transforms the data using Dataproc Serverless with PySpark, stores curated data in both GCS and Cloud Spanner, and performs analytical processing in BigQuery through a Spanner external connection.

> **Note:** This project uses **Dataproc Serverless with PySpark** for data processing. It does **not** use Dataflow or Airflow/DAGs.

---

## GCP Services

| GCP Service                      | Purpose                                      |
| -------------------------------- | -------------------------------------------- |
| **Pub/Sub**                      | Real-time order event ingestion              |
| **Cloud Storage (GCS)**          | Bronze and Silver data layers                |
| **Dataproc Serverless**          | Serverless PySpark data processing           |
| **Cloud Spanner**                | Operational storage for processed order data |
| **BigQuery**                     | Analytical processing and reporting          |
| **BigQuery External Connection** | Federated access to Cloud Spanner            |
| **BigQuery Scheduled Queries**   | Scheduled product-level aggregation          |

---

## Architecture

```text
                    E-Commerce Orders
                           │
                           ▼
                    Python Publisher
                           │
                           ▼
                    Google Cloud Pub/Sub
                           │
                           ▼
                    GCS Bronze Layer
                       Raw JSON
                           │
                           ▼
                 Dataproc Serverless
                       PySpark
                           │
                  ┌────────┴────────┐
                  │                 │
                  ▼                 ▼
             GCS Silver       Cloud Spanner
               Parquet          EcomOrders
                                    │
                                    ▼
                               BigQuery
                           External Connection
                                    │
                                    ▼
                           EXTERNAL_QUERY
                                    │
                                    ▼
                        Product Aggregation
                                    │
                                    ▼
                     daily_product_summary
```

The pipeline uses Pub/Sub for event ingestion and Dataproc Serverless with PySpark for transformation.

PySpark flattens nested product data, converts and validates data types, removes duplicate order/product records, and writes the processed data to both the GCS Silver layer and Cloud Spanner.

BigQuery accesses Cloud Spanner through an external connection and calculates product-level analytical metrics.

---

## Pipeline Flow

1. The Python publisher generates e-commerce order events.
2. Order events are published to Google Cloud Pub/Sub.
3. Pub/Sub delivers the raw events to the GCS Bronze layer.
4. Dataproc Serverless runs the PySpark processing job.
5. PySpark reads the raw JSON data from the Bronze layer.
6. Nested product arrays are flattened into individual order line items.
7. Data types and timestamps are validated and converted.
8. Duplicate `(order_id, product_id)` records are removed.
9. Cleaned data is written as Parquet files to the GCS Silver layer.
10. Processed order records are written to Cloud Spanner.
11. BigQuery connects to Cloud Spanner through an external connection.
12. `EXTERNAL_QUERY` retrieves data from Cloud Spanner.
13. Product-level sales metrics are calculated.
14. Aggregated results are stored in the `daily_product_summary` table.
15. Sanity checks validate data quality and pipeline results.

---

## Data Layers

### 🥉 Bronze — Raw Data

The Bronze layer stores raw e-commerce order events in JSON format.

```text
Pub/Sub
   │
   ▼
GCS Bronze
   │
   ▼
Raw JSON
```

**Purpose:**

* Preserve raw source events
* Maintain the original event structure
* Provide a source layer for downstream processing

---

### 🥈 Silver — Curated Data

The Silver layer contains cleaned, flattened, typed, and deduplicated order data in Parquet format.

```text
GCS Bronze
   │
   ▼
Dataproc Serverless
   │
   ▼
PySpark
   │
   ├── Flatten nested products
   ├── Validate data types
   ├── Convert timestamps
   └── Remove duplicates
   │
   ▼
GCS Silver
   │
   ▼
Parquet
```

**Purpose:**

* Flatten nested order/product structures
* Standardize data types
* Remove duplicate records
* Produce analytics-ready data

---

### ⚙️ Operational Data — Cloud Spanner

Processed order line items are also stored in Cloud Spanner.

```text
PySpark
   │
   ▼
Cloud Spanner
   │
   ▼
EcomOrders
```

Cloud Spanner acts as the operational storage layer for the processed order records.

---

### 🥇 Analytics — BigQuery

BigQuery accesses the Cloud Spanner data using an external connection.

```text
Cloud Spanner
      │
      ▼
BigQuery External Connection
      │
      ▼
EXTERNAL_QUERY
      │
      ▼
Product Aggregation
      │
      ▼
daily_product_summary
```

This allows BigQuery to perform analytical queries against Cloud Spanner without first copying the operational data into a native BigQuery table.

---

## Cloud Spanner Schema

The processed order line-item data is stored in the `EcomOrders` table.

```sql
CREATE TABLE EcomOrders (
    order_id STRING(64) NOT NULL,
    product_id STRING(64) NOT NULL,
    user_id STRING(64),
    quantity INT64,
    order_date TIMESTAMP
) PRIMARY KEY (order_id, product_id);
```

The primary key is:

```text
(order_id, product_id)
```

This key also supports duplicate detection and uniqueness validation for order/product combinations.

---

## BigQuery Analytics

BigQuery performs product-level aggregation using data accessed from Cloud Spanner through the external connection.

The analytical processing calculates:

* Total items sold
* Total orders
* Product-level sales metrics
* Aggregation timestamp

### BigQuery Dataset

```text
ecom_analytics
```

### Final Analytical Table

```text
ecom_analytics.daily_product_summary
```

### Spanner External Connection

```text
spanner-ecom-conn
```

BigQuery uses the `EXTERNAL_QUERY` function to access data from Cloud Spanner through the configured external connection.

---

## Data Quality Checks

The project includes SQL-based sanity checks to validate the processed data and analytical results.

### Volume Checks

Validates:

* Record counts
* Expected data volume
* Product/order counts

### Completeness Checks

Validates:

* Missing records
* Required fields
* Expected data availability

### NULL Checks

Checks required columns for unexpected `NULL` values.

### Schema Integrity

Validates that the processed data follows the expected schema.

### Data Type Validation

Validates fields such as:

* `order_id`
* `product_id`
* `user_id`
* `quantity`
* `order_date`

### Timestamp Validation

Checks timestamps for:

* Missing values
* Invalid values
* Unexpected future timestamps
* Data freshness issues

### Deduplication

Validates that duplicate `(order_id, product_id)` records are not introduced during processing.

### Idempotency

Validates that rerunning the processing logic does not incorrectly create duplicate business records.

### Primary Key Uniqueness

Validates uniqueness of the Cloud Spanner primary key:

```text
(order_id, product_id)
```

### Business Logic

Validates business rules such as:

* Valid quantities
* Expected product/order relationships
* Valid aggregation results

### Data Reconciliation

Compares relevant record counts and metrics between processing stages to identify missing or unexpected records.

### Data Freshness

Validates that the latest available data is within the expected processing window.

### BigQuery–Spanner Connectivity

Validates that BigQuery can successfully access Cloud Spanner through the configured external connection.

These checks help identify missing records, duplicates, invalid data, reconciliation issues, connectivity problems, and stale pipeline results.

---

## Project Configuration

```text
GCP Project ID        : snappy-mapper-498509-e0
Region                : asia-south1

Bronze Bucket         : pub-sub-rawdata
Silver Bucket         : pub-sub-clean-data

Pub/Sub Topic         : ecom-raw-topic
Pub/Sub Subscription  : ecom-raw-gcs-sub

Spanner Instance      : ecom-spanner-instance
Spanner Database      : ecom-db

Spanner Connection    : spanner-ecom-conn

BigQuery Dataset      : ecom_analytics
```

> **Note:** These values represent the environment-specific configuration used by the project. If the repository is public, consider replacing the GCP project ID and resource names with placeholders before publishing.

---

## Repository Structure

```text
📦 gcp-realtime-ecommerce-streaming-pipeline
│
├── 📁 architecture
│   └── 📄 real-time-ecommerce-gcp-pipeline-architecture.png
│
├── 📁 docs
│   ├── 📄 01-theory-and-architecture.pdf
│   ├── 📄 02-code-documentation.pdf
│   └── 📄 03-sanity-checks.pdf
│
├── 📁 src
│   │
│   ├── 📁 publisher
│   │   └── 📄 publisher.py
│   │
│   └── 📁 pyspark
│       └── 📄 clean_ecom.py
│
├── 📁 sql
│   ├── 📄 spanner_schema.sql
│   ├── 📄 bigquery_aggregation.sql
│   └── 📄 verification_queries.sql
│
├── 📁 tests
│   └── 📄 sanity_checks.sql
│
├── 📄 README.md
├── 📄 .gitignore
└── 📄 LICENSE
```

---

## Execution Order

```text
1. Create GCS Bronze and Silver buckets
                    ↓
2. Configure Pub/Sub topic and subscription
                    ↓
3. Create Cloud Spanner instance and database
                    ↓
4. Create the EcomOrders table
                    ↓
5. Run the Python Publisher
                    ↓
6. Verify raw events in GCS Bronze
                    ↓
7. Run Dataproc Serverless with PySpark
                    ↓
8. Verify curated data in GCS Silver
                    ↓
9. Verify processed records in Cloud Spanner
                    ↓
10. Configure the BigQuery-Spanner external connection
                    ↓
11. Create the BigQuery dataset and summary table
                    ↓
12. Run or schedule the BigQuery aggregation
                    ↓
13. Verify the analytical results
                    ↓
14. Run verification and sanity checks
```

---

## Documentation

The `docs/` directory contains detailed project documentation:

* **Theory & Architecture** — architecture, GCP services, data flow, and project setup
* **Code Documentation** — Python publisher, PySpark processing, SQL, and IAM configuration
* **Sanity Checks** — data validation, reconciliation, and pipeline verification

---

## Key Technical Concepts

* Real-time event ingestion
* Event-driven architecture
* Google Cloud Pub/Sub
* Google Cloud Storage
* Bronze / Silver data layers
* Dataproc Serverless
* PySpark
* JSON processing
* Parquet
* Nested data transformation
* Data flattening
* Data deduplication
* Cloud Spanner
* BigQuery federation
* External connections
* `EXTERNAL_QUERY`
* Scheduled queries
* Data reconciliation
* Idempotency
* Data quality validation
* Serverless data processing

---

## Security

Do not commit credentials, secrets, or private keys to GitHub.

Never commit:

```text
Service account private keys
API keys
Passwords
Credentials
.env files
Secret files
```

Use appropriate GCP IAM permissions and follow the principle of least privilege.

Sensitive configuration should be provided through secure configuration or secret-management mechanisms rather than hard-coded in source code.

---

## Project Status

**Proof of Concept (POC)**

This project demonstrates an event-driven e-commerce data pipeline on Google Cloud Platform using Pub/Sub, Cloud Storage, Dataproc Serverless with PySpark, Cloud Spanner, and BigQuery.
