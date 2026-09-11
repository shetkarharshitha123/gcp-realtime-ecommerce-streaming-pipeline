````markdown
# Real-Time E-Commerce GCP Streaming Pipeline

A real-time e-commerce data pipeline built on Google Cloud Platform (GCP).

This project demonstrates real-time event ingestion, data transformation, data storage, federated analytics, and data quality validation using managed GCP services.

> **Note:** This project uses **Dataproc Serverless with PySpark** for data processing. It does not use Dataflow or Airflow/DAGs.

## GCP Services

- **Pub/Sub** — real-time order event ingestion
- **Cloud Storage (GCS)** — Bronze and Silver data layers
- **Dataproc Serverless** — PySpark data processing
- **Cloud Spanner** — operational data storage
- **BigQuery** — analytical processing
- **BigQuery External Connection** — federated access to Spanner
- **BigQuery Scheduled Queries** — daily product aggregation

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
       ├───────────────┐
       ▼               ▼
GCS Silver        Cloud Spanner
  Parquet           EcomOrders
                       │
                       ▼
                  BigQuery
                EXTERNAL_QUERY
                       │
                       ▼
             Daily Product Summary
````

The pipeline uses PySpark to flatten nested product data, convert data types, remove duplicate order/product records, and write the processed data to both GCS Silver and Cloud Spanner. 

## Pipeline Flow

1. Python publisher generates e-commerce order events.
2. Events are published to Google Cloud Pub/Sub.
3. Pub/Sub writes the raw JSON messages to the GCS Bronze bucket.
4. Dataproc Serverless runs the PySpark transformation.
5. PySpark reads and processes the raw JSON data.
6. Nested product arrays are flattened into individual order line items.
7. Data types and timestamps are validated and converted.
8. Duplicate `(order_id, product_id)` records are removed.
9. Cleaned data is stored as Parquet in the GCS Silver layer.
10. Processed records are written to Cloud Spanner.
11. BigQuery connects to Cloud Spanner using `EXTERNAL_QUERY`.
12. Product-level sales metrics are calculated.
13. Results are stored in the `daily_product_summary` table.
14. Sanity checks validate the processed data.

## Data Layers

### Bronze

Raw order events are stored as JSON files.

```text
Pub/Sub
   ↓
GCS Bronze
   ↓
Raw JSON
```

### Silver

Cleaned and deduplicated data is stored in Parquet format.

```text
Raw JSON
   ↓
Dataproc + PySpark
   ↓
GCS Silver
   ↓
Parquet
```

### Operational Data

Processed order line items are stored in Cloud Spanner.

```text
PySpark
   ↓
Cloud Spanner
   ↓
EcomOrders
```

### Analytics

BigQuery queries Cloud Spanner directly through the external connection.

```text
Cloud Spanner
      ↓
BigQuery External Connection
      ↓
EXTERNAL_QUERY
      ↓
Product Aggregation
      ↓
daily_product_summary
```

## Cloud Spanner Schema

```sql
CREATE TABLE EcomOrders (
    order_id STRING(64) NOT NULL,
    product_id STRING(64) NOT NULL,
    user_id STRING(64),
    quantity INT64,
    order_date TIMESTAMP
) PRIMARY KEY (order_id, product_id);
```

## BigQuery Analytics

The BigQuery aggregation calculates:

* Total items sold
* Total orders
* Product-level metrics
* Aggregation timestamp

The final analytical table is:

```text
ecom_analytics.daily_product_summary
```

BigQuery uses the Spanner external connection:

```text
spanner-ecom-conn
```

and the `EXTERNAL_QUERY` function to access Cloud Spanner data. 

## Data Quality Checks

The project includes sanity checks for:

* Data volume
* Data completeness
* NULL values
* Schema integrity
* Data type validation
* Timestamp validation
* Deduplication
* Idempotency
* Primary key uniqueness
* Business logic
* Data reconciliation
* Data freshness
* BigQuery-Spanner connectivity

These checks help identify missing records, duplicates, invalid data, reconciliation issues, and stale pipeline results.  

## Project Configuration

```text
GCP Project ID        : snappy-mapper-498509-e0
Bronze Bucket         : pub-sub-rawdata
Silver Bucket         : pub-sub-clean-data
Pub/Sub Topic         : ecom-raw-topic
Pub/Sub Subscription  : ecom-raw-gcs-sub
Spanner Instance      : ecom-spanner-instance
Spanner Database      : ecom-db
Spanner Connection    : spanner-ecom-conn
BigQuery Dataset      : ecom_analytics
Region                : asia-south1
```

These are the environment values specified in the project documentation.   

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

## Execution Order

```text
1. Create GCS Bronze & Silver buckets
              ↓
2. Configure Pub/Sub
              ↓
3. Create Cloud Spanner
              ↓
4. Run Python Publisher
              ↓
5. Run Dataproc Serverless + PySpark
              ↓
6. Configure BigQuery Spanner Connection
              ↓
7. Create BigQuery Dataset & Summary Table
              ↓
8. Run / Schedule BigQuery Aggregation
              ↓
9. Run Verification & Sanity Checks
```

## Documentation

The `docs/` folder contains:

* **Theory & Architecture** — architecture, services, data flow, and setup
* **Code Documentation** — publisher, PySpark, SQL, and IAM configuration
* **Sanity Checks** — data validation and pipeline verification

## Key Concepts

* Real-time data ingestion
* Event-driven architecture
* Pub/Sub
* GCS Bronze/Silver layers
* Dataproc Serverless
* PySpark
* JSON transformation
* Data deduplication
* Cloud Spanner
* BigQuery federation
* Zero-ETL analytics
* `EXTERNAL_QUERY`
* Scheduled queries
* Data reconciliation
* Data quality validation

## Project Status

**Proof of Concept (POC)**

```

This is the version I would put on GitHub: **professional, readable, and still simple**, without adding technologies that aren't actually part of this project.
```
