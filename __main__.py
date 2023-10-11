from dotenv import load_dotenv
import logging
import os

from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector

logging.basicConfig(filename='pipeline.log', encoding='utf-8', level=logging.DEBUG,
                    format="%(asctime)s [%(levelname)s] %(name)s - %(funcName).40s - %(message)s",)
logger = logging.getLogger(__name__)

load_dotenv()
CARD_DATA_PDF_PATH = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
API_KEY = os.getenv('x-api-key')
NUMBER_STORES_ENDPOINT_URL = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
STORE_ENDPOINT_URL = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'


def setup_database(filename):
    db_conn = DatabaseConnector()
    db_credentials = db_conn.read_db_creds(filename)
    engine = db_conn.init_db_engine(db_credentials)
    return db_conn, engine


if __name__ == '__main__':
    logger.info('****************************** Starting pipeline ******************************')
    db_extractor = DataExtractor()
    cleaner = DataCleaning()
    source_db, source_engine = setup_database(filename='config/db_creds.yaml')
    target_db, target_engine = setup_database(filename='config/db_creds_target.yaml')

    # Extract -> Clean -> Load User data
    # with source_engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
    #     # Uncomment below to list all database tables
    #     # source_db.list_db_tables(source_engine)
    #     extracted = db_extractor.read_rds_table(conn, 'legacy_users')
    #
    # df_users = cleaner.clean_user_data(df=extracted)
    # assert len(df_users.index) == 15284
    # target_db.upload_to_db(target_engine, df=df_users, table_name='dim_users')

    # Extract -> Clean -> Load Card data
    # df_card_details = db_extractor.retrieve_pdf_data(pdf_path=CARD_DATA_PDF_PATH)
    # df_card_details = cleaner.clean_card_data(df=df_card_details)
    # assert len(df_card_details.index) == 15284
    # target_db.upload_to_db(target_engine, df=df_card_details, table_name='dim_card_details')

    # Extract -> Clean -> Load Product data
    headers = {
        # "Content-Type": "application/json",
        "X-API-KEY": API_KEY
    }
    num_stores = db_extractor.list_number_of_stores(url=NUMBER_STORES_ENDPOINT_URL, headers=headers)
    df_stores = db_extractor.retrieve_stores_data(url=STORE_ENDPOINT_URL, headers=headers, number_stores=num_stores)
