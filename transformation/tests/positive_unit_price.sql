-- Test: ensure all unit prices are positive

SELECT
    product_id,
    unit_price
FROM
    {{ ref('stg_products') }}
WHERE
    unit_price < 0