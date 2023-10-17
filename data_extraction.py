import boto3
import botocore
import logging
import pandas as pd
from pathlib import Path
import requests
import tabula
from typing import Dict, Tuple

logger = logging.getLogger(__name__)


class DataExtractor:

    @staticmethod
    def read_rds_table(connection, table_name: str) -> pd.DataFrame:
        """This function accepts an sql connection and table name and returns the table as a dataframe"""
        logger.info(
            f"Read table {table_name} from host {connection.engine.url.host}")
        return pd.read_sql_table(table_name=table_name, con=connection)

    @staticmethod
    def retrieve_pdf_data(pdf_path: str) -> pd.DataFrame:
        """This function accepts a path to a pdf, reads the pdf and returns the contents"""
        logger.info(f"Read pdf file from path {pdf_path}")
        df = pd.DataFrame(columns=[
            'card_number', 'expiry_date', 'card_provider',
            'date_payment_confirmed'
        ])

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
    def retrieve_stores_data(url: str, headers: Dict,
                             number_stores: int) -> pd.DataFrame:
        stores = []

        for number in range(number_stores):
            store_url = f"{url}{number}"
            logger.info(f"Request store data for {store_url}")
            response = requests.get(store_url, headers=headers)
            stores.append(response.json())
            logger.debug(f"Adding store data {response.json()}")
        return pd.DataFrame(stores)

    @staticmethod
    def _parse_s3_url(s3_url) -> Tuple[str, str]:
        """Returns the bucket and key from a s3 url"""
        components = s3_url.split('/')
        bucket, key = components[-2], components[-1]
        return bucket, key

    def extract_from_s3(self, s3_address) -> pd.DataFrame or None:
        """Download file from s3 bucket, returns contents as a dataframe"""
        bucket, key = self._parse_s3_url(s3_address)
        s3 = boto3.client('s3')
        local_path = Path.cwd() / key

        try:
            s3.download_file(bucket, key, local_path)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "403":
                print(f"The s3 object {s3_address} does not exist.")
            else:
                raise
            return None

        if local_path.suffix == '.csv':
            df = pd.read_csv(local_path)
        else:
            df = pd.read_json(local_path)

        local_path.unlink(missing_ok=True)
        return df
