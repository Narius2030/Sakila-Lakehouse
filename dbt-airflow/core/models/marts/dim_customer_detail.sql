{{ config(
    unique_key='customer_id',
    depends_on=['customer_address_detail']
) }}

WITH customer_detail AS (
    SELECT
        c.customer_id, c.store_id, c.active,
        CONCAT(c.first_name, ' ', c.last_name) AS full_name,
        cd.address_id, cd.city, cd.country
    FROM {{ source("delta-streamify", "customer") }} c
    JOIN {{ ref("customer_address_detail") }} cd
        ON cd.address_id = c.address_id
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} AS customer_key,
    *
FROM customer_detail