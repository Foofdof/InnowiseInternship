--3.1
SELECT
	category.name as category_name,
	COUNT(film_id) as film_count
FROM 
	category
LEFT JOIN 
	film_category ON category.category_id = film_category.category_id
GROUP BY 
	category.category_id, 
	category.name
ORDER BY
	film_count DESC;


--3.2
SELECT 
	actor.last_name as last_name,
	actor.first_name as first_name,
	COUNT(rental.inventory_id) as total_rentals
FROM actor
JOIN film_actor ON actor.actor_id = film_actor.actor_id
JOIN inventory ON film_actor.film_id = inventory.inventory_id
JOIN rental ON inventory.inventory_id = rental.inventory_id
GROUP BY
	last_name, first_name
ORDER BY
	total_rentals DESC
LIMIT 10;


--3.3
SELECT
	category.name as category_name,
	SUM(payment.amount) as total_amount
FROM 
	category
JOIN film_category ON category.category_id = film_category.category_id
JOIN inventory ON film_category.film_id = inventory.inventory_id
JOIN rental ON inventory.inventory_id = rental.inventory_id
JOIN payment ON rental.rental_id = payment.rental_id
GROUP BY 
	category.category_id, 
	category.name
ORDER BY
	total_amount DESC
LIMIT 1;


--3.4
SELECT
	film.title
FROM 
	film
LEFT JOIN inventory ON film.film_id=inventory.film_id
WHERE inventory.film_id IS NULL;


--3.5
WITH ranked_actors AS (
    SELECT
        actor.last_name,
        COUNT(DISTINCT film_actor.film_id) AS film_count,
        DENSE_RANK() OVER (ORDER BY COUNT(DISTINCT film_actor.film_id) DESC) AS rank
    FROM category
    JOIN film_category ON category.category_id = film_category.category_id
    JOIN film_actor ON film_category.film_id = film_actor.film_id
    JOIN actor ON film_actor.actor_id = actor.actor_id
    WHERE category.name = 'Children'
    GROUP BY actor.last_name, actor.actor_id
)
SELECT
	last_name,
    film_count
FROM ranked_actors
WHERE rank <= 3
ORDER BY film_count DESC;


--3.6
SELECT 
	city.city_id,
    city.city,
    SUM(customer.active) AS is_active,
    COUNT(customer.active) - SUM(customer.active) AS not_active
FROM customer
LEFT JOIN address ON customer.address_id = address.city_id
JOIN city ON address.city_id = city.city_id 
GROUP BY city.city, city.city_id
ORDER BY not_active DESC;


-- 3.7
SELECT
	sub.cat_name,
	sub.cond_type,
	sub.total_diff
FROM (
	SELECT
		CASE
			WHEN city.city LIKE 'A%' THEN 'starts_with_A'
			WHEN city.city LIKE '%-%' THEN 'has-'
		END as cond_type,
		category.name as cat_name,
		ROW_NUMBER() OVER(
			PARTITION BY 
				CASE
					WHEN city.city LIKE 'A%' THEN 'starts_with_A'
					WHEN city.city LIKE '%-%' THEN 'has-'
				END
			ORDER BY
				SUM(EXTRACT(HOURS FROM rental.return_date - rental.rental_date))
		) as rn,
		SUM(EXTRACT(HOURS FROM rental.return_date - rental.rental_date)) as total_diff
	FROM rental
	JOIN customer ON rental.customer_id = customer.customer_id
	JOIN address ON customer.address_id = address.address_id
	JOIN city ON address.city_id = city.city_id
	JOIN inventory ON rental.inventory_id = inventory.inventory_id
	JOIN film_category ON inventory.film_id = film_category.film_id
	JOIN category ON film_category.category_id = category.category_id
	WHERE city.city LIKE 'A%' OR city.city LIKE '%-%'
	GROUP BY 
		cat_name,
		cond_type
) sub
WHERE sub.rn = 1