SELECT
    CAST(TO_UNIXTIME(date) AS BIGINT) AS date_key,
    date,
    DAY_OF_WEEK(date) AS day_of_week,
    DAY(date) AS day_of_month,
    WEEK(date) AS week_of_year,
    MONTH(date) AS month,
    YEAR(date) AS year,
    CASE WHEN DAY_OF_WEEK(date) IN (6,7) THEN True ELSE False END AS weekend_flag
FROM UNNEST(SEQUENCE(DATE '2009-01-01', DATE '2020-01-01', INTERVAL '1' DAY)) AS t(date)