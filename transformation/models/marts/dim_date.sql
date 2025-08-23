{{
    config(
        materialized='table',
        schema='marts'
    )
}}

SELECT
    CAST(date_day AS DATE) AS date_day,
    EXTRACT(year FROM date_day) AS year,
    EXTRACT(month FROM date_day) AS month,
    EXTRACT(day FROM date_day) AS day_of_month,
    EXTRACT(quarter FROM date_day) AS quarter,
    EXTRACT(isodow FROM date_day) AS day_of_week,
    EXTRACT(week FROM date_day) AS week_of_year,
    (date_day = CURRENT_DATE()) AS is_today,
    (EXTRACT(isodow FROM date_day) IN (6, 7)) AS is_weekend
FROM (
    SELECT
    UNNEST(
        GENERATE_SERIES(
            '2022-01-01'::date,
            CURRENT_DATE()::date,
            INTERVAL '1 day'
        )
    ) AS date_day
) AS series_data