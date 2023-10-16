-- Task 4. The product_price column has a £ character which you need to remove using SQL.
UPDATE dim_products
SET product_price = replace(product_price, '£', '');

-- Add a new column weight_class which will contain human-readable values based on the 
-- weight range of the product.
ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(15);

UPDATE dim_products
SET weight_class = (
       CASE
	       WHEN weight < 2 THEN 'Light'
		   WHEN weight BETWEEN 2 AND 40 THEN 'Mid_Sized'
		   WHEN weight BETWEEN 40 AND 140 THEN 'Heavy'
		   ELSE 'Truck_Required'
		END 
);
