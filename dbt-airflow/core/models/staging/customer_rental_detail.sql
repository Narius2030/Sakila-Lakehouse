{{ config(
		unique_key='customer_id'
) }}


WITH address_detail AS (
	SELECT
		r.rental_id, p.amount, r.customer_id,
		f.title, f.release_year, f.rental_rate, f.rating AS label, f.special_features,
		c.name AS category_name
	FROM rental r
	JOIN {{ source("delta-streamify", "payment") }} p ON p.rental_id=p.rental_id
	JOIN {{ source("delta-streamify", "inventory") }} i ON i.inventory_id=r.inventory_id
	JOIN {{ source("delta-streamify", "film") }} f ON f.film_id=i.film_id
	JOIN {{ source("delta-streamify", "film_category") }} fc ON fc.film_id=f.film_id
	JOIN {{ source("delta-streamify", "category") }} c ON c.category_id=fc.category_id
)

SELECT * FROM address_detail
