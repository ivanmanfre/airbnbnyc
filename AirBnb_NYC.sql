SELECT 
   *
FROM train
LIMIT 10;
-----       DATA CLEANING
-- Looking for NULL or empty string values
-- Count of non NULL values. NULLIF treats empty spaces as NULL ensuring they aren't counted.
SELECT 
    COUNT(*) AS total_rows,
    COUNT(NULLIF(name, '')) AS name_count,
    COUNT(NULLIF(host_name, '')) AS host_name_count,
    COUNT(NULLIF(neighbourhood_group, '')) AS neighbourhood_group_count,
    COUNT(NULLIF(neighbourhood, '')) AS neighbourhood_count,
    COUNT(NULLIF(room_type, '')) AS room_type_count,
    COUNT(latitude) AS latitude_count,
    COUNT(longitude) AS longitude_count,
    COUNT(price) AS price_count,
    COUNT(minimum_nights) AS minimum_nights_count,
    COUNT(number_of_reviews) AS number_of_reviews_count,
    COUNT(NULLIF(last_review, '')) AS last_review_count,
    COUNT(reviews_per_month) AS reviews_per_month_count,
    COUNT(calculated_host_listings_count) AS calculated_host_listings_count_count,
    COUNT(availability_365) AS availability_365_count
FROM train;

-- Total row number is 48895. Isolating the columns that returned less, meaning they have NULL and/or empty values.
SELECT name, host_name, last_review, reviews_per_month
FROM train
LIMIT 5;


-- In case of reviews_per_month, we can replace those NULL/empty values for zero.
UPDATE train
SET reviews_per_month = 0
WHERE reviews_per_month IS NULL;

-- With last_review, NULLs will remain and we will convert the empty strings to NULL as well.
UPDATE train
SET last_review = NULL
WHERE last_review = ''

-- For host_name, we will just replace for 'Unknown'
UPDATE train
SET host_name = 'Unknown'
WHERE host_name IS NULL or host_name = ''
-- For name, we decided to combine other columns like room_type and neighborhood to form a descriptive naming
UPDATE train
SET name = CONCAT(room_type,' ','located at',' ', neighbourhood)
WHERE name IS NULL or name = ''
----

-- Looking for duplicates. Not founded by id only yes by name, host_name and room_type
 WITH ranked AS
    (SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY name, host_name, room_type order by id) AS rn
    FROM train)
	SELECT *
	FROM ranked
	WHERE rn > 1
	---
	-- Using DELETE (deletable CTE)
	WITH ranked AS (
    SELECT id, 
           ROW_NUMBER() OVER (PARTITION BY name, host_name, room_type ORDER BY id) AS rn
    FROM train
),
to_delete AS ( --deleteable CTE
    SELECT id
    FROM ranked
    WHERE rn > 1
)
DELETE FROM train
WHERE id IN (SELECT id FROM to_delete);

-------------------------------------------------
-----------------------------------------------
--- Exploratory

SELECT 
    MIN(Price) AS Min_Price,
    MAX(Price) AS Max_Price,
    AVG(Price) AS Avg_Price,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS Median_Price,
    STDDEV(Price) AS Stddev_Price
FROM 
    train;

SELECT COUNT(*) FROM train; -- Total properties
SELECT neighbourhood_group, COUNT(*) FROM train GROUP BY neighbourhood_group; -- By borough
SELECT room_type, COUNT(*) FROM train GROUP BY room_type; -- by room type

--- Neighbourhood with more listings

SELECT neighbourhood, neighbourhood_group, COUNT(*) AS properties
FROM train
GROUP BY neighbourhood_group, neighbourhood
ORDER BY properties DESC
LIMIT 6

-- Expensive neighbourhoods
SELECT neighbourhood, neighbourhood_group, AVG(price) AS avg_price
FROM train
GROUP BY neighbourhood_group, neighbourhood
ORDER BY avg_price DESC


--
--- Reviews x price
SELECT number_of_reviews, AVG(price)
FROM train
GROUP BY number_of_reviews
ORDER BY number_of_reviews DESC
 
-- 
--- Host Analysis
-- Which hosts has the most listings?


SELECT DISTINCT host_id, host_name, calculated_host_listings_count
FROM train 
ORDER BY calculated_host_listings_count DESC
LIMIT 5;

-- AVG Price vs Host listings
SELECT DISTINCT host_id, host_name, calculated_host_listings_count, ROUND(AVG(Price),0) as avg_price
FROM train 
GROUP BY host_id, host_name, calculated_host_listings_count
ORDER BY calculated_host_listings_count DESC
LIMIT 5;

--------- Availability Trend Analysis
-- Listing through the year

SELECT availability_365, COUNT(*) AS number_of_listings
FROM train 
GROUP BY availability_365
ORDER BY availability_365 DESC