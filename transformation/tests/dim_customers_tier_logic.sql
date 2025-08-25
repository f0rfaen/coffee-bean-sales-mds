-- Test that the customer_tier is correctly assigned based on has_loyalty_card.

SELECT
    customer_id,
    has_loyalty_card,
    customer_tier
FROM
    {{ ref('dim_customers') }}
WHERE
    (has_loyalty_card = TRUE AND customer_tier != 'Loyal Customer')
    OR (has_loyalty_card = FALSE AND customer_tier != 'Standard')
