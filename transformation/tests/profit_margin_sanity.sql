-- Test: Ensure profit is less than the unit price

SELECT
    product_id,
    unit_price,
    profit
FROM
    {{ ref('stg_products') }}
WHERE
    profit >= unit_price