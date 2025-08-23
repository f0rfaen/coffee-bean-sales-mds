{{
    config(
        materialized='table',
        schema='marts'
    )
}}

SELECT
    order_date,
    SUM(total_sales) AS total_daily_sales,
    SUM(quantity) AS total_daily_quantity,
    SUM(profit) AS total_daily_profit,
    COUNT(DISTINCT order_id) AS total_daily_orders
FROM
    {{ ref('int_orders_enriched') }}
GROUP BY
    order_date
ORDER BY
    order_date