{{ config(materialized = 'view', schema='staging') }}

SELECT
    toString(product_id) AS product_id,
    CASE
        WHEN trim(lower(coffee_type)) LIKE 'ara%' THEN 'arabica'
        WHEN trim(lower(coffee_type)) LIKE 'exc%' THEN 'excelsa'
        WHEN trim(lower(coffee_type)) LIKE 'lib%' THEN 'liberica'
        WHEN trim(lower(coffee_type)) LIKE 'rob%' THEN 'robusta'
        ELSE trim(lower(coffee_type))
    END AS coffee_type,
    CASE
        WHEN trim(lower(roast_type)) = 'd' THEN 'dark'
        WHEN trim(lower(roast_type)) = 'm' THEN 'medium'
        WHEN trim(lower(roast_type)) = 'l' THEN 'light'
        ELSE trim(lower(roast_type))
    END AS roast_type,
    toInt64(size) AS size,
    toFloat64(unit_price) AS unit_price,
    toFloat64(price_per_100g) AS price_per_100g,
    toFloat64(profit) AS profit
FROM raw.products
WHERE
    product_id IS NOT NULL