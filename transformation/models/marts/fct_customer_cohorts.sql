{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH customer_first_order AS (
    SELECT
        customer_id,
        MIN(order_date) AS first_order_date
    FROM
        {{ ref('fct_orders') }}
    GROUP BY customer_id
),

customer_monthly_sales AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', order_date) AS sales_month,
        SUM(total_sales) AS total_monthly_sales,
        COUNT(DISTINCT order_id) AS monthly_order_count
    FROM
        {{ ref('fct_orders') }}
    GROUP BY 1, 2
)

SELECT
    cmo.customer_id,
    cmo.sales_month,
    cfo.first_order_date,
    (EXTRACT(YEAR FROM cmo.sales_month) * 12 + EXTRACT(MONTH FROM cmo.sales_month))
    - (EXTRACT(YEAR FROM cfo.first_order_date) * 12 + EXTRACT(MONTH FROM cfo.first_order_date)) AS cohort_period,
    cmo.total_monthly_sales,
    cmo.monthly_order_count
FROM
    customer_monthly_sales AS cmo
LEFT JOIN
    customer_first_order AS cfo
    ON cmo.customer_id = cfo.customer_id
ORDER BY
    cmo.customer_id, cmo.sales_month
