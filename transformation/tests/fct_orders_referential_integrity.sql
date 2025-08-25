-- Test for referential integrity between fct_orders and dim_customers.

SELECT
    f.customer_sk
FROM
    {{ ref('fct_orders') }} AS f
LEFT JOIN
    {{ ref('dim_customers') }} AS d
    ON f.customer_sk = d.customer_sk
WHERE
    d.customer_sk IS NULL
