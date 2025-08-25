{{ config(materialized = 'view', schema='staging') }}
SELECT
    CAST(product_id AS VARCHAR) AS product_id,
    CASE
        WHEN TRIM(LOWER(coffee_type)) LIKE 'ara%' THEN 'arabica'
        WHEN TRIM(LOWER(coffee_type)) LIKE 'exc%' THEN 'excelsa'
        WHEN TRIM(LOWER(coffee_type)) LIKE 'lib%' THEN 'liberica'
        WHEN TRIM(LOWER(coffee_type)) LIKE 'rob%' THEN 'robusta'
        ELSE TRIM(LOWER(coffee_type))
    END AS coffee_type,
    CASE
        WHEN TRIM(LOWER(roast_type)) = 'd' THEN 'dark'
        WHEN TRIM(LOWER(roast_type)) = 'm' THEN 'medium'
        WHEN TRIM(LOWER(roast_type)) = 'l' THEN 'light'
        ELSE TRIM(LOWER(roast_type))
    END AS roast_type,
    CAST(size AS INTEGER) AS size,
    CAST(unit_price AS DECIMAL(10, 2)) AS unit_price,
    CAST(price_per_100g AS DECIMAL(10, 2)) AS price_per_100g,
    CAST(profit AS DECIMAL(10, 2)) AS profit,
FROM {{ source('raw', 'products') }}
WHERE
    product_id IS NOT NULL
    