{{
    config(
        materialized='table',
        schema='intermediate'
    )
}}

SELECT
    o.order_id,
    o.order_date,
    o.customer_id,
    o.product_id,
    o.quantity,
    o.quantity * p.unit_price AS total_sales,
    p.unit_price,
    p.coffee_type,
    p.roast_type,
    p.size,
    p.profit
FROM
    {{ ref('stg_orders') }} AS o
LEFT JOIN
    {{ ref('stg_products') }} AS p
    ON o.product_id = p.product_id