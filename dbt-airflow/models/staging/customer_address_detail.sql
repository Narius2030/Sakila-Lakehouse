{{ config(
    unique_key='address_id'
) }}

WITH address_detail AS (
    SELECT 
        ad.address_id, c.city, ad.postal_code, co.country
    FROM {{ source("delta-streamify", "address") }} ad
    JOIN  {{ source("delta-streamify", "city") }} c 
        ON c.city_id=ad.city_id
    JOIN {{ source("delta-streamify", "country") }} co 
        ON co.country_id = c.country_id
)

SELECT * FROM address_detail
