-- Test that the total_sales column is consistent with quantity and unit_price.

SELECT
    order_id,
    total_sales,
    quantity,
    unit_price
FROM
    {{ ref('int_orders_enriched') }}
WHERE
    total_sales != quantity * unit_price
