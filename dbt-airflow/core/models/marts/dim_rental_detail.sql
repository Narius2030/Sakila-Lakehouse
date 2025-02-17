{{ config(
    unique_key='rental_id'
) }}

WITH rental_detail AS (
    SELECT 
        r.rental_id, r.rental_date, r.inventory_id,	r.customer_id, r.rental_month, 
        r.return_date, r.staff_id, p.amount, p.payment_date
    FROM {{ source("delta-streamify", "rental") }} r
    JOIN {{ source("delta-streamify", "payment") }} p
        ON p.rental_id = r.rental_id
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['rental_id']) }} AS rental_key,
    * 
FROM rental_detail