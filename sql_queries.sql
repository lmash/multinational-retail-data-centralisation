-- Task 1. How many stores does the business have & in which countries
SELECT country_code AS country, 
       COUNT(*) AS total_no_stores
FROM  
    dim_store_details
WHERE 
    longitude IS NOT NULL
GROUP BY
    country_code
ORDER BY 
    total_no_stores DESC;
	
-- Task 2. Which locations currently have the most stores?
SELECT locality,
       COUNT(*) AS total_no_stores
FROM
    dim_store_details
GROUP BY
    locality
ORDER BY
    total_no_stores DESC,
	locality
LIMIT 7;

-- Task 3. Which months product the average highest cost of sales typically?
SELECT ROUND(SUM(ord.product_quantity * prod.product_price)::numeric, 2) AS total_sales,
       dt.month
FROM
    orders_table ord
INNER JOIN
    dim_products prod ON ord.product_code = prod.product_code
INNER JOIN
    dim_date_times dt ON ord.date_uuid = dt.date_uuid
GROUP BY 
    dt.month
ORDER BY
    total_sales DESC
LIMIT 
    6;

-- Task 4. How many sales are coming from online?
SELECT COUNT(*) AS number_of_sales,
       SUM(ord.product_quantity) AS product_quantity_count,
	   (
	       CASE
	           WHEN store.store_type = 'Web Portal' THEN 'Web'
		       ELSE 'Offline'
	        END
	   ) AS location
FROM
    orders_table ord
INNER JOIN
    dim_products prod ON ord.product_code = prod.product_code
INNER JOIN
    dim_store_details store ON ord.store_code = store.store_code
GROUP BY
    location
ORDER BY
    number_of_sales;
    
-- Task 5. What percentage of sales come through each type of store?
WITH all_stores AS (
	SELECT SUM(ord.product_quantity * prod.product_price) AS sales
	FROM
		orders_table ord
	INNER JOIN
		dim_products prod ON ord.product_code = prod.product_code
	INNER join
		dim_store_details store ON ord.store_code = store.store_code	
),sales_per_store AS (
	SELECT store.store_type,
		   ROUND(SUM(ord.product_quantity * prod.product_price)::numeric, 2) AS total_sales 
	FROM
		orders_table ord
	INNER JOIN
		dim_products prod ON ord.product_code = prod.product_code
	INNER join
		dim_store_details store ON ord.store_code = store.store_code
	GROUP BY store.store_type
)SELECT sales_per_store.store_type,
        sales_per_store.total_sales,
		ROUND((sales_per_store.total_sales / all_stores.sales * 100)::numeric, 2) AS "percentage_total(%)"
FROM
    all_stores,
	sales_per_store
ORDER BY 
    sales_per_store.total_sales DESC;

-- Task 6. Which month in each year produced the most sales
SELECT ROUND(SUM(ord.product_quantity * prod.product_price)::numeric, 2) AS total_sales,
       dt.year,
	   dt.month
FROM
    orders_table ord
INNER JOIN
    dim_products prod ON ord.product_code = prod.product_code
INNER JOIN
    dim_date_times dt ON ord.date_uuid = dt.date_uuid
GROUP BY
    dt.year,
	dt.month
ORDER BY 
    total_sales DESC
LIMIT
    10;

-- Task 7. What is our staff headcount?
SELECT SUM(staff_numbers) AS total_staff_numbers,
       country_code
FROM dim_store_details
GROUP BY
    country_code
ORDER BY
    total_staff_numbers DESC;

-- Task 8. Which German store type is selling the most?
SELECT ROUND(SUM(ord.product_quantity * prod.product_price)::numeric, 2) AS total_sales,
       store_type,
	   country_code
FROM
    orders_table ord
INNER JOIN
    dim_products prod ON ord.product_code = prod.product_code
INNER JOIN
    dim_store_details store ON ord.store_code = store.store_code
GROUP BY
    store_type,
	country_code
HAVING
    country_code = 'DE'
ORDER BY
    total_sales;

-- Task 9. How quickly is the company making sales?
