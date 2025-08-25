-- Test that aggregated values in agg_monthly_sales are not negative.

SELECT
    sales_month,
    roast_type,
    number_of_orders,
    total_quantity_sold,
    total_monthly_sales
FROM
    {{ ref('agg_monthly_sales') }}
WHERE
    number_of_orders < 0
    OR total_quantity_sold < 0
    OR total_monthly_sales < 0
