from dataclasses import dataclass
from typing import List

OZ_TO_KG = 35.274
G_TO_KG = 1000
M_TO_KG = .001


@dataclass
class ColumnEntries:
    column_name: str
    entries: List


valid_months = ColumnEntries(
    column_name='month',
    entries=['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']
)
valid_categories = ColumnEntries(
    column_name='category',
    entries=['toys-and-games', 'sports-and-leisure', 'pets', 'homeware', 'health-and-beauty', 'food-and-drink', 'diy']
)
valid_country_codes = ColumnEntries(
    column_name='country_code',
    entries=['DE', 'GB', 'US']
)
valid_card_providers = ColumnEntries(
    column_name='card_provider',
    entries=[
        'Diners Club / Carte Blanche', 'American Express', 'JCB 16 digit',
        'JCB 15 digit', 'Maestro', 'Mastercard', 'Discover', 'VISA 19 digit',
        'VISA 16 digit', 'VISA 13 digit']
)


@dataclass
class Endpoints:
    card_data: str
    number_of_stores: str
    store_details: str
    products: str
    date_times: str


endpoints = Endpoints(
    card_data='https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf',
    number_of_stores='https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores',
    store_details='https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/',
    products='s3://data-handling-public/products.csv',
    date_times='s3://data-handling-public/date_details.json'
)


@dataclass
class DataType:
    name: str
    extracted_count: int
    clean_count: int
    table_name: str
    column_entries: ColumnEntries or None


card = DataType(name='Card',
                extracted_count=15309,
                clean_count=15284,
                table_name='dim_card_details',
                column_entries=valid_card_providers
                )

user = DataType(name='User',
                extracted_count=15320,
                clean_count=15284,
                table_name='dim_users',
                column_entries=valid_country_codes
                )

store = DataType(name='Store',
                 extracted_count=451,
                 clean_count=441,
                 table_name='dim_store_details',
                 column_entries=valid_country_codes
                 )

product = DataType(name='Product',
                   extracted_count=1853,
                   clean_count=1846,
                   table_name='dim_products',
                   column_entries=valid_categories
                   )

order = DataType(name='Order',
                 extracted_count=120123,
                 clean_count=120123,
                 table_name='orders_table',
                 column_entries=None
                 )

date_times = DataType(name='Date_Times',
                      extracted_count=120161,
                      clean_count=120123,
                      table_name='dim_date_times',
                      column_entries=valid_months
                      )
