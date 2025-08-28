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
    GROUP BY
        customer_id

),

customer_monthly_sales AS (

    SELECT
        customer_id,
        toStartOfMonth(order_date) AS sales_month,
        SUM(total_sales) AS total_monthly_sales,
        COUNT(DISTINCT order_id) AS monthly_order_count
    FROM
        {{ ref('fct_orders') }}
    GROUP BY
        customer_id, sales_month

)

SELECT
    cmo.customer_id,
    cmo.sales_month,
    cfo.first_order_date,
    (toYear(cmo.sales_month) * 12 + toMonth(cmo.sales_month)) -
    (toYear(cfo.first_order_date) * 12 + toMonth(cfo.first_order_date)) AS cohort_period,
    cmo.total_monthly_sales,
    cmo.monthly_order_count
FROM
    customer_monthly_sales AS cmo
LEFT JOIN
    customer_first_order AS cfo
    ON cmo.customer_id = cfo.customer_id
ORDER BY
    cmo.customer_id, cmo.sales_month