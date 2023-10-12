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
S3_ADDRESS = 's3://data-handling-public/products.csv'


def setup_database(filename):
    db_conn = DatabaseConnector()
    db_credentials = db_conn.read_db_creds(filename)
    engine = db_conn.init_db_engine(db_credentials)
    return db_conn, engine


def process_product_data(target_db, target_engine):
    """Extract -> Clean -> Load Product data"""
    print(f"Processing Product Data")
    extractor, cleaner = DataExtractor(), DataCleaning()

    df_products = extractor.extract_from_s3(s3_address=S3_ADDRESS)
    df_products = cleaner.clean_product_data(df=df_products)
    assert len(df_products.index) == 1846
    target_db.upload_to_db(target_engine, df=df_products, table_name='dim_products')


def process_store_data(target_db, target_engine):
    """Extract -> Clean -> Load Store data"""
    print(f"Processing Store Data")
    extractor, cleaner = DataExtractor(), DataCleaning()

    headers = {
        "Content-Type": "application/json",
        "X-API-KEY": API_KEY
    }
    num_stores = extractor.list_number_of_stores(url=NUMBER_STORES_ENDPOINT_URL, headers=headers)
    df_stores = extractor.retrieve_stores_data(url=STORE_ENDPOINT_URL, headers=headers, number_stores=num_stores)
    df_stores = cleaner.clean_store_data(df=df_stores)
    assert len(df_stores.index) == 441
    target_db.upload_to_db(target_engine, df=df_stores, table_name='dim_store_details')


def process_card_data(target_db, target_engine):
    """Extract -> Clean -> Load Card data"""
    print(f"Processing Card Data")
    extractor, cleaner = DataExtractor(), DataCleaning()

    df_card_details = extractor.retrieve_pdf_data(pdf_path=CARD_DATA_PDF_PATH)
    df_card_details = cleaner.clean_card_data(df=df_card_details)
    assert len(df_card_details.index) == 15284
    target_db.upload_to_db(target_engine, df=df_card_details, table_name='dim_card_details')


def process_user_data(source_db, source_engine, target_db, target_engine):
    """Extract -> Clean -> Load User data"""
    print(f"Processing User Data")
    extractor, cleaner = DataExtractor(), DataCleaning()

    with source_engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
        # Uncomment below to list all database tables
        # source_db.list_db_tables(source_engine)
        extracted = extractor.read_rds_table(conn, 'legacy_users')

    df_users = cleaner.clean_user_data(df=extracted)
    assert len(df_users.index) == 15284
    target_db.upload_to_db(target_engine, df=df_users, table_name='dim_users')


if __name__ == '__main__':
    logger.info('****************************** Starting pipeline ******************************')
    src_db, src_engine = setup_database(filename='config/db_creds.yaml')
    tgt_db, tgt_engine = setup_database(filename='config/db_creds_target.yaml')

    process_user_data(src_db, src_engine, tgt_db, tgt_engine)
    process_card_data(tgt_db, tgt_engine)
    process_store_data(tgt_db, tgt_engine)
    process_product_data(tgt_db, tgt_engine)

