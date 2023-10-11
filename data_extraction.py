import logging
import pandas as pd
import requests
import tabula
from typing import Dict

logger = logging.getLogger(__name__)


class DataExtractor:
    @staticmethod
    def read_rds_table(connection, table_name: str) -> pd.DataFrame:
        """This function accepts an sql connection and table name and returns the table as a dataframe"""
        logger.info(f"Read table {table_name} from host {connection.engine.url.host}")
        # df = pd.read_sql_table(table_name=table_name, con=connection)
        return pd.read_sql_table(table_name=table_name, con=connection)

    @staticmethod
    def retrieve_pdf_data(pdf_path: str) -> pd.DataFrame:
        """This function accepts a path to a pdf, reads the pdf and returns the contents"""
        logger.info(f"Read pdf file from path {pdf_path}")
        df = pd.DataFrame(columns=['card_number', 'expiry_date', 'card_provider', 'date_payment_confirmed'])

        cards = tabula.read_pdf(pdf_path, stream=True, pages="all")
        for card in cards:
            df = pd.concat([df, card])

        # Reset the index as each page has a new index starting from 0
        df.reset_index(inplace=True, drop=True)
        return df

    @staticmethod
    def list_number_of_stores(url: str, headers: Dict) -> int:
        response = requests.get(url, headers=headers)
        return response.json()['number_stores']

    @staticmethod
    def retrieve_stores_data(url: str, headers: Dict, number_stores: int) -> pd.DataFrame:
        stores = []

        for number in range(number_stores):
            store_url = f"{url}{number}"
            logger.info(f"Request store data for {store_url}")
            response = requests.get(store_url, headers=headers)
            stores.append(response.json())
            logger.debug(f"Adding store data {response.json()}")
        return pd.DataFrame(stores)
