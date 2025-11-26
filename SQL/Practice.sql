-- Output the number of movies in each category, sorted descending.

SELECT 
	c.name,
	COUNT(fc.film_id) AS number_of_movies
FROM film_category AS fc
JOIN category AS c
	ON fc.category_id = c.category_id
GROUP BY c.name
ORDER BY number_of_movies DESC;

-- Output the 10 actors whose movies rented the most, sorted in descending order.

WITH actor_names AS (
    SELECT
        actor_id,
        first_name || ' ' || last_name AS actor_name
    FROM actor
)
SELECT
    an.actor_name,
    COUNT(r.rental_id) AS number_of_rentals
FROM actor_names AS an
JOIN film_actor AS fa
    USING (actor_id)
JOIN inventory AS i
    USING (film_id)
JOIN rental AS r
    USING (inventory_id)
GROUP BY an.actor_name
ORDER BY number_of_rentals DESC
LIMIT 10;

-- Output the category of movies on which the most money was spent.

SELECT 
	c.name AS category_name,
	SUM(p.amount) AS money_per_category
FROM category AS c
JOIN film_category AS fc
	USING (category_id)
JOIN inventory AS i
    USING (film_id)
JOIN rental AS r
    USING (inventory_id)
JOIN payment AS p
	USING (rental_id)
GROUP BY c.name
ORDER BY money_per_category DESC
LIMIT 1;

-- Print the names of movies that are not in the inventory. 
-- Write a query without using the IN operator.

SELECT f.title
FROM film AS f
WHERE NOT EXISTS (
	SELECT 1 
	FROM inventory AS i 
	WHERE i.film_id = f.film_id);

-- Output the top 3 actors who have appeared the most in movies in the “Children” category. 
-- If several actors have the same number of movies, output all of them.

WITH children_actors AS (
    SELECT
        a.first_name || ' ' || a.last_name AS actor_name,
        COUNT(fc.film_id) AS number_of_movies
    FROM actor AS a
    JOIN film_actor AS fa 
		USING (actor_id)
    JOIN film_category AS fc 
		USING (film_id)
    JOIN category AS c 
		USING (category_id)
    WHERE c.name = 'Children'
    GROUP BY actor_name
)
SELECT
    actor_name,
    number_of_movies,
	actor_rank
FROM (
    SELECT
        actor_name,
		number_of_movies,
        DENSE_RANK() OVER (ORDER BY number_of_movies DESC) AS actor_rank
    FROM children_actors
) AS ranked_actors
WHERE actor_rank <= 3;

-- Output cities with the number of active and inactive customers (active - customer.active = 1). 
-- Sort by the number of inactive customers in descending order.

SELECT 
	ct.city,
	SUM(cm.active) AS active,
	SUM(CASE WHEN cm.active = 1 THEN 0 ELSE 1 END) AS inactive
FROM city AS ct
JOIN address AS ad
	USING (city_id)
JOIN customer AS cm
	USING (address_id)
GROUP BY ct.city
ORDER BY inactive DESC;

-- Output the category of movies that have the highest number of total rental hours 
-- in the city (customer.address_id in this city) and that start with the letter “a”. 
-- Do the same for cities that have a “-” in them. 
-- Write everything in one query.

WITH category_durations AS (
	SELECT 
		c.name AS category_name,
		ct.city,
		SUM(r.return_date - r.rental_date) AS duration
	FROM category AS c
	JOIN film_category AS fc
		USING (category_id)
	JOIN inventory AS i
		USING (film_id)
	JOIN rental AS r
		USING (inventory_id)
	JOIN customer AS cm
		USING (customer_id)
	JOIN address AS ad
		USING (address_id)
	JOIN city AS ct
		USING (city_id)
	WHERE ct.city ILIKE 'a%' OR ct.city LIKE '%-%'
	GROUP BY ct.city, c.name
),
max_categories AS (
    SELECT
        category_name,
        city,
        duration,
        MAX(duration) OVER (PARTITION BY city) AS max_category
    FROM category_durations
)
SELECT
    category_name,
    city,
    duration
FROM max_categories
WHERE duration = max_category
ORDER BY city;

