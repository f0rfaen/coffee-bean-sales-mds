-- Test for gaps in the date sequence in dim_date.

WITH date_diffs AS (
    SELECT
        date_day,
        LAG(date_day, 1) OVER (ORDER BY date_day) AS prev_date
    FROM
        {{ ref('dim_date') }}
)
SELECT
    date_day,
    prev_date
FROM
    date_diffs
WHERE
    date_day != DATE_ADD(prev_date, INTERVAL '1 day')
    AND prev_date IS NOT NULL
