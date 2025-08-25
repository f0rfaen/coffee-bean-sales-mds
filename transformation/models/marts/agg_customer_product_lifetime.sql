{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH customer_product_sales AS (

    SELECT
        customer_id,
        product_id,
        MIN(order_date) AS first_purchase_date,
        MAX(order_date) AS last_purchase_date,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(quantity) AS total_quantity_purchased,
        SUM(total_sales) AS total_lifetime_spend
    FROM
        {{ ref('fct_orders') }}
    GROUP BY
        customer_id, product_id

)

SELECT
    c.customer_id,
    c.first_purchase_date,
    c.last_purchase_date,
    c.total_orders,
    c.total_quantity_purchased,
    c.total_lifetime_spend,
    p.coffee_type,
    p.roast_type
FROM
    customer_product_sales AS c
LEFT JOIN
    {{ ref('dim_products') }} AS p
    ON c.product_id = p.product_id