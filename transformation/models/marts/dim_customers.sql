{{
    config(
        materialized='table',
        schema='marts'
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} AS customer_sk,
    customer_id,
    first_name,
    last_name,
    COALESCE(email, 'unknown') AS email,
    country,
    has_loyalty_card,
    CASE
        WHEN has_loyalty_card = TRUE THEN 'Loyal Customer'
        ELSE 'Standard'
    END AS customer_tier
FROM
    {{ ref('stg_customers') }}