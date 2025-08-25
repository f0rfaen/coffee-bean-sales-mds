{{
    config(
        materialized='table',
        schema='marts'
    )
}}

SELECT
    product_id,
    coffee_type,
    roast_type,
    size,
    unit_price,
    price_per_100g,
    profit
FROM
    {{ ref('stg_products') }}