#!/bin/zsh
set -x

psql -U postgres -d sales_data < sql_scripts/0_drop_tables.sql
python __main__.py
psql -U postgres -d sales_data < sql_scripts/1_orders_table_cast_columns.sql
psql -U postgres -d sales_data < sql_scripts/2_dim_users_table_cast_columns.sql
psql -U postgres -d sales_data < sql_scripts/3_dim_store_details_cast_columns.sql
psql -U postgres -d sales_data < sql_scripts/4_update_dim_products.sql
psql -U postgres -d sales_data < sql_scripts/5_dim_products_cast_columns.sql
psql -U postgres -d sales_data < sql_scripts/6_dim_date_times_cast_columns.sql
psql -U postgres -d sales_data < sql_scripts/7_dim_card_details_cast_columns.sql
psql -U postgres -d sales_data < sql_scripts/8_create_primary_keys.sql
psql -U postgres -d sales_data < sql_scripts/9_create_foreign_keys_on_orders_table.sql

set +x
