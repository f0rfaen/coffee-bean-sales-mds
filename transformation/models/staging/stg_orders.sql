{{ config(materialized = 'view', schema='staging') }}

SELECT 
    toString(order_id) AS order_id,
    toString(customer_id) AS customer_id,
    toString(product_id) AS product_id,
    toDate(order_date) AS order_date,
    toInt64(quantity) AS quantity
FROM raw.orders
WHERE
    order_id IS NOT NULL