{{
    config(
        materialized='table',
        schema='marts'
    )
}}

SELECT
    c.customer_sk,
    o.customer_id,
    o.order_id,
    o.order_date,
    o.quantity,
    o.unit_price,
    o.total_sales,
    o.product_id
FROM
    {{ ref('int_orders_enriched') }} AS o
LEFT JOIN
    {{ ref('dim_customers') }} AS c
    ON o.customer_id = c.customer_id