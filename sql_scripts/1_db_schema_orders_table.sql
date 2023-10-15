-- Task 1. Cast orders_table columns to correct data types
ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;

ALTER TABLE orders_table
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid;

ALTER TABLE orders_table
ALTER COLUMN card_number TYPE VARCHAR(19);

ALTER TABLE orders_table
ALTER COLUMN store_code TYPE VARCHAR(12);

ALTER TABLE orders_table
ALTER COLUMN product_code TYPE VARCHAR(11);

ALTER TABLE orders_table
ALTER COLUMN product_quantity TYPE SMALLINT;

-- Task 2. Cast dim_users columns to correct data types
ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255);

ALTER TABLE dim_users
ALTER COLUMN last_name TYPE VARCHAR(255);

ALTER TABLE dim_users
ALTER COLUMN date_of_birth TYPE DATE;

ALTER TABLE dim_users
ALTER COLUMN country_code TYPE VARCHAR(2);

ALTER TABLE dim_users
ALTER COLUMN user_uuid TYPE UUID using user_uuid::uuid;

ALTER TABLE dim_users
ALTER COLUMN join_date TYPE DATE;

-- Task 3. Cast dim_users columns to correct data types
UPDATE dim_store_details
SET latitude = CONCAT(latitude, lat);

ALTER TABLE dim_store_details
DROP COLUMN IF EXISTS lat;

-- Note have to do work below to update the N/A's to NULL in pandas ( I think..)
-- update dim_store_details
-- SET longitude = NULL
-- where longitude = 'N/A';

ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT USING longitude::double precision;

ALTER TABLE dim_store_details
ALTER COLUMN locality TYPE VARCHAR(255);

ALTER TABLE dim_store_details
ALTER COLUMN store_code TYPE VARCHAR(12);

ALTER TABLE dim_store_details
ALTER COLUMN staff_numbers TYPE SMALLINT;

ALTER TABLE dim_store_details
ALTER COLUMN opening_date TYPE DATE;

-- PLease ask question about nullable allowed??
ALTER TABLE dim_store_details
ALTER COLUMN store_type TYPE VARCHAR(255);

-- Note have to do work below to update the N/A's to NULL in pandas ( I think..)
-- update dim_store_details
-- SET latitude = NULL
-- where latitude = 'N/A';

ALTER TABLE dim_store_details
ALTER column latitude TYPE FLOAT USING latitude::double precision;

SELECT *
from dim_store_details
-- WHERE lat is not NULL
LIMIT 10;

CREATE TABLE dim_store_details_bkp AS
SELECT * FROM dim_store_details;

CREATE TABLE dim_store_details AS
SELECT * FROM dim_store_details_bkp;

DROP TABLE dim_store_details;