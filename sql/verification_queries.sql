SELECT  
  COUNT(*) AS total_summary_rows, 
  SUM(total_items_sold) AS total_units_sold, 
  SUM(total_orders) AS total_orders_processed, 
  MAX(aggregated_at) AS last_updated_at 
FROM `snappy-mapper-498509e0.ecom_analytics.daily_product_summary 

