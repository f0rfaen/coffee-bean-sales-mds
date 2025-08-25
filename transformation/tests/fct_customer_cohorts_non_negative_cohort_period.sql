-- Test that the cohort_period is never negative.

SELECT
    customer_id,
    sales_month,
    first_order_date,
    cohort_period
FROM
    {{ ref('fct_customer_cohorts') }}
WHERE
    cohort_period < 0
