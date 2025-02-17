{{ config(
    depends_on=['dim_datetime', 'dim_customer_detail', 'dim_rental_detail']
) }}


WITH fact_customer_segment AS (
    SELECT
        dt.date_key,
        c.customer_key, 
        r.rental_key,
        c.city, 
        c.country, 
        c.active, 
        c.full_name, 
        r.amount, 
        r.rental_date,

        -- Dùng MIN và MAX trong window function
        MIN(DATE(r.rental_date)) OVER (PARTITION BY c.customer_id) AS first_date,
        MAX(DATE(r.rental_date)) OVER (PARTITION BY c.customer_id) AS lasted_date,
        DATE_DIFF(
            'day',
            MIN(r.rental_date) OVER (PARTITION BY c.customer_id),
            MAX(r.rental_date) OVER (PARTITION BY c.customer_id)
        ) AS recency,

        -- Dùng SUM và COUNT trong window function
        SUM(r.amount) OVER (PARTITION BY c.customer_id) AS monetary,
        COUNT(r.rental_id) OVER (PARTITION BY c.customer_id) AS frequency

    FROM {{ ref("dim_customer_detail") }} c
    JOIN {{ ref("dim_rental_detail") }} r 
        ON r.customer_id = c.customer_id
    JOIN {{ ref("dim_datetime") }} dt
        ON dt.date = DATE(r.rental_date)
)

SELECT * FROM fact_customer_segment