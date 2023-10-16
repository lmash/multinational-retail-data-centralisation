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