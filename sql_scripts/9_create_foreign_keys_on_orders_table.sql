-- Task 9. Add foreign keys to the orders table
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_table_card_number FOREIGN KEY (card_number) REFERENCES dim_card_details;

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_table_date_uuid FOREIGN KEY (date_uuid) REFERENCES dim_date_times;

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_table_product_code FOREIGN KEY (product_code) REFERENCES dim_products;

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_table_store_code FOREIGN KEY (store_code) REFERENCES dim_store_details;

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_table_user_uuid FOREIGN KEY (user_uuid) REFERENCES dim_users;