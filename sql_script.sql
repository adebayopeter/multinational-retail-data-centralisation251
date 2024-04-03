/********************************
Milestone 3: Task 1 (orders_table)
********************************/

-- Show current `orders_table` table structure
SELECT column_name, data_type FROM information_schema.columns 
WHERE table_name = 'orders_table';

-- Convert `date_uuid` column to UUID data type
ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

-- Convert `user_uuid` column to UUID data type
ALTER TABLE orders_table
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;

-- Get MAXIMUM length in `card_number`, `store_code` and `product_code`
SELECT 
MAX(LENGTH(CAST(card_number AS VARCHAR))) AS max_card_number_length, 
MAX(LENGTH(CAST(store_code AS VARCHAR))) AS max_store_code_length, 
MAX(LENGTH(CAST(product_code AS VARCHAR))) AS max_product_code_length
FROM public.orders_table;

-- Convert `card_number`, `store_code` and `product_code` columns to VARCHAR data type
ALTER TABLE orders_table
ALTER COLUMN card_number TYPE VARCHAR(19);

ALTER TABLE orders_table
ALTER COLUMN store_code TYPE VARCHAR(12);

ALTER TABLE orders_table
ALTER COLUMN product_code TYPE VARCHAR(11);

ALTER TABLE orders_table
ALTER COLUMN product_quantity TYPE SMALLINT;


/********************************
Milestone 3: Task 2 (dim_users)
********************************/
-- Show current `dim_users` table structure
SELECT column_name, data_type FROM information_schema.columns 
WHERE table_name = 'dim_users';

-- Get MAXIMUM length in `country_code`
SELECT 
MAX(LENGTH(CAST(country_code AS VARCHAR))) AS max_country_code
FROM public.dim_users;

-- Convert `first_name`, `last_name`, `date_of_birth`, `country_code`, `user_uuid` and `join_date` columns to required data type
ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255);

ALTER TABLE dim_users
ALTER COLUMN last_name TYPE VARCHAR(255);

ALTER TABLE dim_users
ALTER COLUMN date_of_birth TYPE DATE;

ALTER TABLE dim_users
ALTER COLUMN country_code TYPE VARCHAR(3);

ALTER TABLE dim_users
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;

ALTER TABLE dim_users
ALTER COLUMN join_date TYPE DATE;

 
 
/***************************************
Milestone 3: Task 3 (dim_store_details)
***************************************/
-- Show current `dim_store_details` table structure
SELECT column_name, data_type FROM information_schema.columns 
WHERE table_name = 'dim_store_details';

-- Get MAXIMUM length in `store_code` and `country_code`
SELECT 
	MAX(LENGTH(CAST(store_code AS VARCHAR))) AS max_store_code
FROM public.dim_store_details;

SELECT 
	MAX(LENGTH(CAST(country_code AS VARCHAR))) AS max_country_code
FROM public.dim_store_details;
 
-- Convert `longitude`, `locality`, `store_code`, `staff_numbers`, `opening_date`, `store_type`, `latitude`, `country_code` and `continent` columns to required data type
ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT;

ALTER TABLE dim_store_details
ALTER COLUMN locality TYPE VARCHAR(255);

ALTER TABLE dim_store_details
ALTER COLUMN store_code TYPE VARCHAR(12);

ALTER TABLE dim_store_details
ALTER COLUMN staff_numbers TYPE SMALLINT;

ALTER TABLE dim_store_details
ALTER COLUMN opening_date TYPE DATE;

ALTER TABLE dim_store_details
ALTER COLUMN store_type TYPE VARCHAR(255);

ALTER TABLE dim_store_details
ALTER COLUMN latitude TYPE FLOAT;

ALTER TABLE dim_store_details
ALTER COLUMN country_code TYPE VARCHAR(2);

ALTER TABLE dim_store_details
ALTER COLUMN continent TYPE VARCHAR(255);

/***************************************
Milestone 3: Task 4 (dim_products)
***************************************/
SELECT column_name, data_type FROM information_schema.columns
WHERE table_name = 'dim_products'

SELECT LENGTH('Truck_Required')

-- Create new column `weight_class`
ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(14)

UPDATE public.dim_products
SET weight_class = 
CASE 
	WHEN weight < 2 THEN 'Light'
	WHEN weight > 2 AND weight < 40 THEN 'Mid_Sized'
	WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
	ELSE 'Truck_Required'
END;

/***************************************
Milestone 3: Task 5 (dim_products)
***************************************/
SELECT column_name, data_type FROM information_schema.columns
WHERE table_name = 'dim_products'

-- Get MAXIMUM length in `EAN`, `product_code` and `weight_class`
SELECT 
	MAX(LENGTH(CAST('EAN' AS VARCHAR))) AS max_ean,
	MAX(LENGTH(CAST(product_code AS VARCHAR))) AS max_product_code,
	MAX(LENGTH(CAST(weight_class AS VARCHAR))) AS max_weight_class
FROM public.dim_products;

-- Rename `removed` column to `still_available`
ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

-- Convert `product_price`, `weight`, `EAN`, `product_code`, `date_added`, `uuid`, `still_available`, and `weight_class` columns to required data type
ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT;

ALTER TABLE dim_products
ALTER COLUMN weight TYPE FLOAT;

ALTER TABLE dim_products
ALTER COLUMN EAN TYPE VARCHAR(14);

ALTER TABLE dim_products
ALTER COLUMN product_code TYPE VARCHAR(11);

ALTER TABLE dim_products
ALTER COLUMN date_added TYPE DATE;

ALTER TABLE dim_products
ALTER COLUMN uuid TYPE UUID USING uuid::UUID;

ALTER TABLE dim_products
ALTER COLUMN still_available TYPE BOOLEAN
USING CASE 
	WHEN still_available = 'Still_avaliable' THEN TRUE
	WHEN still_available = 'Removed' THEN FALSE
	ELSE NULL
END;

ALTER TABLE dim_products
ALTER COLUMN weight_class TYPE VARCHAR(14);

SELECT  * FROM dim_products


/***************************************
Milestone 3: Task 6 (dim_date_times)
***************************************/
SELECT column_name, data_type FROM information_schema.columns
WHERE table_name = 'dim_date_times'

-- Get MAXIMUM length in `EAN`, `product_code` and `weight_class`
SELECT 
	MAX(LENGTH(CAST(year AS VARCHAR))) AS max_year,
	MAX(LENGTH(CAST(day AS VARCHAR))) AS max_day,
	MAX(LENGTH(CAST(time_period AS VARCHAR))) AS max_time_period
FROM public.dim_date_times;

-- Convert `year`, `day`, `time_period`, and `date_uuid` columns to required data type
ALTER TABLE dim_date_times
ALTER COLUMN year TYPE VARCHAR(4);

ALTER TABLE dim_date_times
ALTER COLUMN day TYPE VARCHAR(2);

ALTER TABLE dim_date_times
ALTER COLUMN time_period TYPE VARCHAR(10);

ALTER TABLE dim_date_times
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;


/***************************************
Milestone 3: Task 7 (dim_card_details)
***************************************/
SELECT column_name, data_type FROM information_schema.columns
WHERE table_name = 'dim_card_details'

SELECT
	MAX(LENGTH(CAST(card_number AS VARCHAR))) AS max_card_number,
	MAX(LENGTH(CAST(expiry_date AS VARCHAR))) AS max_expiry_date
FROM public.dim_card_details

ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(19);

ALTER TABLE dim_card_details
ALTER COLUMN expiry_date TYPE VARCHAR(5);

ALTER TABLE dim_card_details
ALTER COLUMN date_payment_confirmed TYPE DATE;

/***************************************
Milestone 3: Task 8 (orders_table, dim_card_details, dim_store_details, dim_products, dim_users)
***************************************/
SELECT column_name, data_type FROM information_schema.columns
WHERE table_name = 'orders_table'

-- Add primary key to dim_card_details
ALTER TABLE dim_card_details
ADD CONSTRAINT dim_card_details_pk PRIMARY KEY (card_number);

-- Add primary key to dim_store_details
ALTER TABLE dim_store_details
ADD CONSTRAINT dim_store_details_pk PRIMARY KEY (store_code);

-- Add primary key to dim_products
ALTER TABLE dim_products
ADD CONSTRAINT dim_products_pk PRIMARY KEY (product_code);

-- Add primary key to dim_users
ALTER TABLE dim_users
ADD CONSTRAINT dim_users_pk PRIMARY KEY (user_uuid);

SELECT * FROM information_schema.key_column_usage
WHERE table_name = 'dim_card_details' AND column_name = 'card_number';

SELECT * FROM information_schema.key_column_usage
WHERE table_name = 'dim_store_details' AND column_name = 'store_code';

SELECT * FROM information_schema.key_column_usage
WHERE table_name = 'dim_products' AND column_name = 'product_code';

SELECT * FROM information_schema.key_column_usage
WHERE table_name = 'dim_users' AND column_name = 'user_uuid';


/***************************************
Milestone 3: Task 9 (orders_table, dim_card_details, dim_store_details, dim_products, dim_users)
***************************************/
-- Add foreign key constraint referencing dim_user_details table
ALTER TABLE orders_table
ADD CONSTRAINT fk_card_number 
FOREIGN KEY (card_number)
REFERENCES dim_card_details(card_number);

-- Add foreign key constraint referencing dim_store_details table
ALTER TABLE orders_table
ADD CONSTRAINT fk_store_code
FOREIGN KEY (store_code)
REFERENCES dim_store_details(store_code);

-- Add foreign key constraint referencing dim_products table
ALTER TABLE dim_products
ADD CONSTRAINT fk_product_code
FOREIGN KEY (product_code)
REFERENCES dim_products(product_code);

--Add foreign key constraint referencing dim_users table
ALTER TABLE dim_users
ADD CONSTRAINT fk_dim_users
FOREIGN KEY (user_uuid)
REFERENCES dim_users(user_uuid);

/***************************************
Milestone 4: Task 1
***************************************/
SELECT country_code, COUNT(country_code) AS total_no_stores 
FROM dim_store_details
GROUP BY country_code
ORDER BY total_no_stores DESC;

/***************************************
Milestone 4: Task 2
***************************************/

SELECT locality, COUNT(locality) AS total_no_stores 
FROM dim_store_details
GROUP BY locality
HAVING COUNT(locality) >= 10
ORDER BY total_no_stores DESC;

SELECT locality, COUNT(locality) AS total_no_stores 
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC
LIMIT 7;

/***************************************
Milestone 4: Task 3
***************************************/
SELECT ROUND(SUM(CAST(o.product_quantity * d.product_price AS numeric)), 2) AS total_sales, dt.month
FROM orders_table o
INNER JOIN dim_products d
ON d.product_code = o.product_code
INNER JOIN dim_date_times dt
ON dt.date_uuid = o.date_uuid
GROUP BY dt.month
ORDER BY total_sales DESC
LIMIT 6;

SELECT ROUND(SUM(CAST(o.product_quantity * d.product_price AS numeric)), 2) AS total_sales, dt.month
FROM orders_table o
INNER JOIN dim_products d
ON d.product_code = o.product_code
INNER JOIN dim_date_times dt
ON dt.date_uuid = o.date_uuid
GROUP BY dt.month
HAVING ROUND(SUM(CAST(o.product_quantity * d.product_price AS numeric)), 2) >= '645303.62'
ORDER BY total_sales DESC
LIMIT 6;


/***************************************
Milestone 4: Task 4
***************************************/
SELECT 
	COUNT(*) AS number_of_sales,
	SUM(o.product_quantity) AS product_quantity_count,
	CASE
		WHEN s.store_type = 'Web Portal' THEN 'Web' 
		ELSE 'Offline'
	END AS location
FROM 
	orders_table o 
INNER JOIN 
	dim_store_details s ON s.store_code = o.store_code
GROUP BY 
	CASE
		WHEN s.store_type = 'Web Portal' THEN 'Web' 
		ELSE 'Offline'
	END
ORDER BY location DESC;

/***************************************
Milestone 4: Task 5
***************************************/
SELECT 
    s.store_type,
    ROUND(SUM(CAST(o.product_quantity * d.product_price AS numeric)), 2) AS total_sales,
    ROUND((SUM(CAST(o.product_quantity * d.product_price AS numeric)) / total_revenue) * 100, 2) AS "percentage_total(%)"
FROM 
    orders_table o 
INNER JOIN 
    dim_store_details s ON s.store_code = o.store_code
INNER JOIN 
    dim_products d ON d.product_code = o.product_code
CROSS JOIN 
    (SELECT ROUND(SUM(CAST(o.product_quantity * d.product_price AS numeric)), 2) AS total_revenue 
     FROM orders_table o 
     INNER JOIN dim_products d ON d.product_code = o.product_code) AS total_sales
GROUP BY 
    s.store_type, total_revenue
ORDER BY 
    total_sales DESC;

/***************************************
Milestone 4: Task 6
***************************************/
SELECT ROUND(SUM(CAST(o.product_quantity * d.product_price AS numeric)), 2) AS total_sales, dt.year, dt.month
FROM orders_table o
INNER JOIN dim_products d
ON d.product_code = o.product_code
INNER JOIN dim_date_times dt
ON dt.date_uuid = o.date_uuid
GROUP BY dt.year, dt.month
ORDER BY total_sales DESC
LIMIT 10;

/***************************************
Milestone 4: Task 7
***************************************/
SELECT 
	SUM(staff_numbers) AS total_staff_numbers, 
	country_code
FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC;

/***************************************
Milestone 4: Task 8
***************************************/
SELECT 
    ROUND(SUM(CAST(o.product_quantity * d.product_price AS numeric)), 2) AS total_sales,
	s.store_type,
	s.country_code
FROM 
    orders_table o 
INNER JOIN 
    dim_store_details s ON s.store_code = o.store_code
INNER JOIN 
    dim_products d ON d.product_code = o.product_code
WHERE s.country_code = 'DE'
GROUP BY 
    s.store_type, s.country_code
ORDER BY 
    total_sales ASC;


/***************************************
Milestone 4: Task 9
***************************************/
SELECT
    year,
    JSON_BUILD_OBJECT(
        'hours', AVG(hours),
        'minutes', AVG(minutes),
        'seconds', AVG(seconds),
        'milliseconds', AVG(milliseconds)
    ) AS actual_time_taken
FROM (
    SELECT
        year,
        EXTRACT(HOUR FROM "timestamp") AS hours,
        EXTRACT(MINUTE FROM "timestamp") AS minutes,
        EXTRACT(SECOND FROM "timestamp") AS seconds,
        EXTRACT(MILLISECOND FROM "timestamp") AS milliseconds
    FROM
        dim_date_times
) AS time_data
GROUP BY
    year
ORDER BY
    year;