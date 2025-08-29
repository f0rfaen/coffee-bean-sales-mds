{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH series_data AS (
    SELECT date_day
    FROM UNNEST(
        sequence(
            DATE '2022-01-01',
            CURRENT_DATE,
            INTERVAL '1' DAY
        )
    ) AS t(date_day)
)

SELECT
    date_day,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(DAY FROM date_day) AS day_of_month,
    EXTRACT(QUARTER FROM date_day) AS quarter,
    -- ISO-style weekday: 1 = Monday, 7 = Sunday
    (EXTRACT(DAY_OF_WEEK FROM date_day) + 5) % 7 + 1 AS day_of_week,
    EXTRACT(WEEK FROM date_day) AS week_of_year,
    (date_day = CURRENT_DATE) AS is_today,
    ((EXTRACT(DAY_OF_WEEK FROM date_day) + 5) % 7 + 1) IN (6,7) AS is_weekend
FROM series_data
