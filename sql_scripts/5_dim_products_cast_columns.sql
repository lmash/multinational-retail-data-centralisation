-- Task 5. Update the dim_products table with the quired data types
ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT USING product_price::double precision;

ALTER TABLE dim_products
ALTER COLUMN weight TYPE FLOAT;

ALTER TABLE dim_products
ALTER COLUMN "EAN" TYPE VARCHAR(19);

ALTER TABLE dim_products
ALTER COLUMN product_code TYPE VARCHAR(11);

ALTER TABLE dim_products
ALTER COLUMN date_added TYPE DATE;

ALTER TABLE dim_products
ALTER COLUMN "uuid" TYPE UUID USING uuid::uuid;

ALTER TABLE dim_products
RENAME removed TO still_available;

ALTER TABLE dim_products
ALTER COLUMN still_available TYPE BOOLEAN USING 
    CASE
	    WHEN still_available = 'Still_avaliable' THEN TRUE
		ELSE FALSE
	END;
