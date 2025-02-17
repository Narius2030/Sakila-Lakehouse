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

SELECT * FROM customer_detail