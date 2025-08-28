-- Test for outlier values in daily profit.
{{
    config(
        severity='warn'
    )
}}

WITH daily_profit_stats AS (
    SELECT
        AVG(total_daily_profit) AS mean_profit,
        stddevPop(total_daily_profit) AS stddev_profit
    FROM
        {{ ref('agg_daily_profitability') }}
)
SELECT
    d.order_date,
    d.total_daily_profit
FROM
    {{ ref('agg_daily_profitability') }} AS d
CROSS JOIN
    daily_profit_stats AS s
WHERE
    ABS(d.total_daily_profit - s.mean_profit) > (3 * s.stddev_profit)