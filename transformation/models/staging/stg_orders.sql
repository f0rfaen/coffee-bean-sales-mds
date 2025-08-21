{{ config(materialized = 'view') }}

SELECT 
    CAST(order_id AS VARCHAR) AS order_id,
    CAST(customer_id AS VARCHAR) AS customer_id,
    CAST(product_id AS VARCHAR) AS product_id,
    CAST(order_date AS DATE) AS order_date,
    CAST(quantity AS INTEGER) AS quantity,
FROM {{ source('raw', 'orders') }}
WHERE
    order_id IS NOT NULL