-- Task 3. Cast dim_users columns to correct data types
ALTER TABLE dim_store_details
DROP COLUMN IF EXISTS lat;

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

ALTER TABLE dim_store_details
ALTER COLUMN store_type TYPE VARCHAR(255);

ALTER TABLE dim_store_details
ALTER COLUMN latitude TYPE FLOAT USING latitude::double precision;

ALTER TABLE dim_store_details
ALTER COLUMN country_code TYPE VARCHAR(2);

ALTER TABLE dim_store_details
ALTER COLUMN continent TYPE VARCHAR(255);

UPDATE dim_store_details
SET address = 'N/A',
	locality = 'N/A'
WHERE store_type = 'Web Portal';