#!zsh
set -x
cd sql_scripts
psql -U postgres -d sales_data < 1_orders_table_cast_columns.sql
psql -U postgres -d sales_data < 2_dim_users_table_cast_columns.sql 
psql -U postgres -d sales_data < 3_dim_store_details_cast_columns.sql 
psql -U postgres -d sales_data < 4_update_dim_products.sql 
psql -U postgres -d sales_data < 5_dim_products_cast_columns.sql
psql -U postgres -d sales_data < 6_dim_date_times_cast_columns.sql 
psql -U postgres -d sales_data < 7_dim_card_details_cast_columns.sql 
psql -U postgres -d sales_data < 8_create_primary_keys.sql 
psql -U postgres -d sales_data < 9_create_foreign_keys_on_orders_table.sql 

cd ../
set +x
