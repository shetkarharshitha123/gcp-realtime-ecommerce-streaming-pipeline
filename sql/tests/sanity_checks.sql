-- Check 1A: Count total rows in Spanner 
SELECT COUNT(*) AS spanner_row_count 
FROM EXTERNAL_QUERY( 
  "projects/snappy-mapper-498509-e0/locations/asiasouth1/connections/spanner-ecom-conn", 
  "SELECT order_id FROM EcomOrders" 
); 
 
-- Check 1B: Count total rows in the BigQuery summary table 
SELECT COUNT(*) AS bq_summary_row_count 
FROM `snappy-mapper-498509-
e0.ecom_analytics.daily_product_summary`; 

-- Check2: Are there any missing key values? 
-- Ensures critical fields like order_id or product_id aren't coming through as empty (NULL). 
SELECT  
  COUNTIF(order_id IS NULL) AS missing_order_ids, 
  COUNTIF(product_id IS NULL) AS missing_product_ids, 
  COUNTIF(quantity IS NULL) AS missing_quantities 
FROM EXTERNAL_QUERY( 
  "projects/snappy-mapper-498509-e0/locations/asiasouth1/connections/spanner-ecom-conn", 
  "SELECT order_id, product_id, quantity FROM EcomOrders" ); 


-- Uniqueness Check3: Are there duplicate orders? 
-- Checks if PySpark deduplication worked so primary key pairs (order_id + product_id) aren't repeated. 
SELECT    order_id,    product_id,  
  COUNT(*) AS duplicate_count 
FROM EXTERNAL_QUERY( 
  "projects/snappy-mapper-498509-e0/locations/asiasouth1/connections/spanner-ecom-conn", 
  "SELECT order_id, product_id FROM EcomOrders" 
) 
GROUP BY order_id, product_id 
HAVING COUNT(*) > 1; 

-- Business Logic Check4 : Do the numbers make sense? 
Ensures metric values aren't zero or negative, and checks if totals match across databases. 
 
-- Check 4A: Look for bad quantities (<= 0) 
SELECT COUNT(*) AS bad_quantity_rows 
FROM EXTERNAL_QUERY( 
  "projects/snappy-mapper-498509-e0/locations/asiasouth1/connections/spanner-ecom-conn", 
  "SELECT quantity FROM EcomOrders WHERE quantity <= 0" 
); 
 
-- Check 4B: Total items sold in Spanner vs BigQuery 
(Reconciliation) 
SELECT  
  (SELECT SUM(quantity) FROM EXTERNAL_QUERY( 
     "projects/snappy-mapper-498509-e0/locations/asiasouth1/connections/spanner-ecom-conn",      "SELECT quantity FROM EcomOrders" 
  )) AS total_units_in_spanner, 
   
  (SELECT SUM(total_items_sold)  
   FROM `snappy-mapper-498509e0.ecom_analytics.daily_product_summary` 
  ) AS total_units_in_bigquery; 

  
-- Data Freshness Check: When was the pipeline last updated? 
-- Checks the newest timestamp in your analytical data mart to ensure the schedule isn't frozen. 
 
SELECT  
  MAX(aggregated_at) AS newest_data_time, 
  CURRENT_TIMESTAMP() AS current_time 
FROM `snappy-mapper-498509e0.ecom_analytics.daily_product_summary`; 


