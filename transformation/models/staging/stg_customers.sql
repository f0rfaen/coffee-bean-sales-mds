{{ config(materialized='view') }}

SELECT 
    CAST(customer_id AS VARCHAR) AS customer_id,
    TRIM(SUBSTRING(customer_name, 1, POSITION(' ' IN customer_name) - 1)) AS first_name,
    TRIM(SUBSTRING(customer_name, POSITION(' ' IN customer_name) + 1)) AS last_name,
    TRIM(LOWER(email)) AS email,
    TRIM(phone_number) AS phone_number,
    TRIM(address_line_1) AS address_line_1,
    TRIM(city) AS city,
    TRIM(country) AS country,
    TRIM(postcode) AS postcode,
    CAST(loyalty_card AS BOOLEAN) AS has_loyalty_card
FROM {{ source('raw', 'customers') }}
WHERE
    customer_id IS NOT NULL
