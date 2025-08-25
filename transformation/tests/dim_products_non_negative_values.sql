-- Test that price and profit columns in dim_products are not negative.

SELECT
    product_id,
    unit_price,
    price_per_100g,
    profit
FROM
    {{ ref('dim_products') }}
WHERE
    unit_price < 0
    OR price_per_100g < 0
    OR profit < 0
