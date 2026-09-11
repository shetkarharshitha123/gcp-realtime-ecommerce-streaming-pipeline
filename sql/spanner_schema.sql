CREATE TABLE EcomOrders (
    order_id STRING(64) NOT NULL,
    product_id STRING(64) NOT NULL,
    user_id STRING(64),
    quantity INT64,
    order_date TIMESTAMP
) PRIMARY KEY (order_id, product_id);
