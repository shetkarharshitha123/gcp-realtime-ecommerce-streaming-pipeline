CREATE TABLE IF NOT EXISTS `snappy-mapper-498509
e0.ecom_analytics.daily_product_summary` AS 
SELECT  
product_id, 
SUM(quantity) AS total_items_sold, 
COUNT(DISTINCT order_id) AS total_orders, 
CURRENT_TIMESTAMP() AS aggregated_at 
FROM EXTERNAL_QUERY( 
"projects/snappy-mapper-498509-e0/locations/asia-south1/connections/spanner-ecom
conn", 
"SELECT order_id, product_id, quantity FROM EcomOrders" 
) 
GROUP BY product_id; 


-- Schedule the Incremental Job: 
-- Paste the append query in the editor: 
 
INSERT INTO `snappy-mapper-498509-e0.ecom_analytics.daily_product_summary` ( 
product_id, 
total_items_sold, 
total_orders, 
aggregated_at 
) 
SELECT  
product_id, 
SUM(quantity) AS total_items_sold, 
COUNT(DISTINCT order_id) AS total_orders, 
CURRENT_TIMESTAMP() AS aggregated_at 
FROM EXTERNAL_QUERY( 
"projects/snappy-mapper-498509-e0/locations/asia-south1/connections/spanner-ecom
conn", 
"SELECT order_id, product_id, quantity FROM EcomOrders" 
) 
GROUP BY product_id;
