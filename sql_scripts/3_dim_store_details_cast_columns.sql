-- Task 3. Cast dim_users columns to correct data types
-- Need to ask about the below as I can't see any beneft from concating the columns, and the empty string 
-- needs to be set back to null anyway to be able to change the column to a float.
-- UPDATE dim_store_details
-- SET latitude = CONCAT(latitude, lat);

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

-- Please ask question about nullable allowed??
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