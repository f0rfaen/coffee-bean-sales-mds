-- Test that aggregated values in agg_daily_profitability are not negative.

SELECT
    order_date,
    total_daily_sales,
    total_daily_quantity,
    total_daily_profit,
    total_daily_orders
FROM
    {{ ref('agg_daily_profitability') }}
WHERE
    total_daily_sales < 0
    OR total_daily_quantity < 0
    OR total_daily_profit < 0
    OR total_daily_orders < 0
