{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH date_series AS (
    SELECT
        toDate('2022-01-01') + number AS date_day
    FROM
        system.numbers
    LIMIT toInt32(today() - toDate('2022-01-01') + 1)
)

SELECT
    date_day,
    toYear(date_day) AS year,
    toMonth(date_day) AS month,
    toDayOfMonth(date_day) AS day_of_month,
    toQuarter(date_day) AS quarter,
    toDayOfWeek(date_day) AS day_of_week,
    toWeek(date_day) AS week_of_year,
    (date_day = today()) AS is_today,
    (toDayOfWeek(date_day) IN (6, 7)) AS is_weekend
FROM
    date_series