-- Test that first_purchase_date is not after last_purchase_date.

SELECT
    customer_id,
    first_purchase_date,
    last_purchase_date
FROM
    {{ ref('agg_customer_product_lifetime') }}
WHERE
    first_purchase_date > last_purchase_date
