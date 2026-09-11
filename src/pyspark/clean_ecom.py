 import sys import json from google.cloud import storage from pyspark.sql import SparkSession from pyspark.sql.functions import col, explode, to_timestamp 
 def main(): 
    # 1. Initialize Spark Session     spark = SparkSession.builder \ 
        .appName("ECommerce Clean Ecom Pipeline") \ 
        .getOrCreate() 
     spark.sparkContext.setLogLevel("WARN") 
 
    # Explicit GCP & Pipeline Configuration     project_id = "snappy-mapper-498509-e0"     raw_bucket_name = "pub-sub-rawdata"     cleaned_parquet_dest = "gs://pub-sub-cleandata/parquet/" 
     
    # Cloud Spanner Target Configuration     spanner_instance_id = "ecom-spanner-instance"     spanner_database_id = "ecom-db"     spanner_table_name = "EcomOrders" 
 
    print(f"[STEP 1] Fetching raw JSON objects from bucket: gs://{raw_bucket_name}/") 
 
    try: 
        # 2. Read raw files using google-cloud-storage Python SDK 
        # This completely bypasses Hadoop's Java URI ':' parsing exception         storage_client = storage.Client(project=project_id)         bucket = storage_client.bucket(raw_bucket_name)         blobs = list(bucket.list_blobs(prefix="raw_order_")) 
         if not blobs: 
            print(f"[WARNING] No raw files found with prefix 
'raw_order_' in gs://{raw_bucket_name}/")             spark.stop()             return 
         json_payloads = []         for blob in blobs: 
            file_content = blob.download_as_text()             if file_content.strip(): 
                json_payloads.append(file_content.strip()) 
 
        print(f"[SUCCESS] Ingested {len(json_payloads)} raw order payloads from GCS.") 
 
        # 3. Parallelize JSON strings into Spark RDD & create DataFrame 
        raw_rdd = spark.sparkContext.parallelize(json_payloads)         raw_df = spark.read.json(raw_rdd) 
         if raw_df.rdd.isEmpty() or "id" not in raw_df.columns:             print("[WARNING] JSON payloads do not contain valid order schema fields.")             spark.stop()             return 
 
        # 4. Clean, explode nested product line items, cast types, and deduplicate         flattened_df = raw_df \ 
            .filter(col("id").isNotNull()) \ 
            .withColumn("product", explode("products")) \ 
            .select(                 col("id").cast("string").alias("order_id"),                 col("userId").cast("string").alias("user_id"), 
                col("product.productId").cast("string").alias("product_id"), 
                col("product.quantity").cast("long").alias("quantity"),                 to_timestamp(col("date")).alias("order_date") 
            ) \ 
            .dropDuplicates(["order_id", "product_id"]) 
         record_count = flattened_df.count() 
        print(f"[SUCCESS] Cleaned and transformed {record_count} product line items.") 
 
        # 5. Write Silver Layer Parquet directly to GCS 
        print(f"[STEP 2] Writing Parquet files to: 
{cleaned_parquet_dest}")         flattened_df.write \ 
            .mode("overwrite") \ 
            .parquet(cleaned_parquet_dest) 
        print("[SUCCESS] Parquet files written to GCS Silver Layer!") 
 
        # 6. Write directly to Cloud Spanner (OLTP) 
        print(f"[STEP 3] Writing records to Spanner database '{spanner_database_id}' -> table '{spanner_table_name}'...")         flattened_df.write \ 
            .format("cloud-spanner") \ 
            .option("projectId", project_id) \ 
            .option("instanceId", spanner_instance_id) \ 
            .option("databaseId", spanner_database_id) \ 
            .option("table", spanner_table_name) \ 
            .mode("append") \ 
            .save() 
 
        print("[SUCCESS] Pipeline completed successfully! Data persisted to GCS Parquet and Cloud Spanner.") 
     except Exception as err: 
        print(f"[ERROR] Pipeline execution failed: {err}")         raise err 
     finally: 
        spark.stop() 
 if __name__ == "__main__": 
    main() 
