-- Test: Check for duplicate email addresses in the customers staging model

SELECT
    email,
    COUNT(*) AS email_count
FROM
    {{ ref('stg_customers') }}
WHERE
    email IS NOT NULL
GROUP BY
    email
HAVING
    COUNT(*) > 1