-- Test: Ensure all order_date entries are not in the future

SELECT
    order_id,
    order_date
FROM
    {{ ref('stg_orders') }}
WHERE
    order_date > CURRENT_DATE()