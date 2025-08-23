{{ config(materialized = 'view', schema='staging') }}
SELECT
    CAST(product_id AS VARCHAR) AS product_id,
    TRIM(LOWER(coffee_type)) AS coffee_type,
    TRIM(LOWER(roast_type)) AS roast_type,
    CAST(size AS INTEGER) AS size,
    CAST(unit_price AS DECIMAL(10, 2)) AS unit_price,
    CAST(price_per_100g AS DECIMAL(10, 2)) AS price_per_100g,
    CAST(profit AS DECIMAL(10, 2)) AS profit,
FROM {{ source('raw', 'products') }}
WHERE
    product_id IS NOT NULL
    