{{
    config(
        materialized='table',
        schema='marts'
    )
}}

SELECT
    DATE_TRUNC('month', order_date) AS sales_month,
    roast_type,
    COUNT(DISTINCT order_id) AS number_of_orders,
    SUM(quantity) AS total_quantity_sold,
    SUM(total_sales) AS total_monthly_sales
FROM
    {{ ref('int_orders_enriched') }}
GROUP BY
    1, 2
ORDER BY
    1