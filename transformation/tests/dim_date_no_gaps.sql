SELECT
    a.date_day
FROM
    {{ ref('dim_date') }} AS a
LEFT JOIN
    {{ ref('dim_date') }} AS b
        ON toDate(a.date_day + 1) = b.date_day
WHERE
    b.date_day IS NULL
    AND a.date_day < today()