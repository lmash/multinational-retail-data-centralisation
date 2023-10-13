from dotenv import load_dotenv
import logging
import os

from config import date_times_config, order_config, product_config, store_config, card_config, \
    user_config, valid_months, valid_categories, valid_country_codes, valid_card_providers, endpoint
from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector

logging.basicConfig(filename='pipeline.log', encoding='utf-8', level=logging.DEBUG,
                    format="%(asctime)s [%(levelname)s] %(name)s - %(funcName).40s - %(message)s",)
logger = logging.getLogger(__name__)

load_dotenv()
API_KEY = os.getenv('x-api-key')


def setup_database(filename):
    db_conn = DatabaseConnector()
    db_credentials = db_conn.read_db_creds(filename)
    engine = db_conn.init_db_engine(db_credentials)
    return db_conn, engine


def process_date_times_data(target_db, target_engine):
    """Extract -> Clean -> Load Product data"""
    print(f"Processing Date & Times Data")
    extractor = DataExtractor()
    cleaner = DataCleaning(column_entries=valid_months)

    df_date_times = extractor.extract_from_s3(s3_address=endpoint.date_times)
    print(f"Date time rows extracted: {len(df_date_times.index)}")
    assert len(df_date_times.index) == date_times_config.extracted_count

    df_date_times = cleaner.clean_date_times_data(df=df_date_times)
    print(f"Date time rows after cleaning: {len(df_date_times.index)}")
    assert len(df_date_times.index) == date_times_config.clean_count

    target_db.upload_to_db(target_engine, df=df_date_times, table_name='dim_date_times')


def process_order_data(source_engine, target_db, target_engine):
    """Extract -> Clean -> Load Order data"""
    print(f"Processing Order Data")
    extractor, cleaner = DataExtractor(), DataCleaning()

    with source_engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
        df_orders = extractor.read_rds_table(conn, 'orders_table')
    print(f"Order rows extracted: {len(df_orders.index)}")
    assert len(df_orders.index) == order_config.extracted_count

    df_orders = cleaner.clean_order_data(df=df_orders)
    print(f"Order rows after cleaning: {len(df_orders.index)}")
    assert len(df_orders.index) == order_config.clean_count

    target_db.upload_to_db(target_engine, df=df_orders, table_name='orders_table')


def process_product_data(target_db, target_engine):
    """Extract -> Clean -> Load Product data"""
    print(f"Processing Product Data")
    extractor = DataExtractor()
    cleaner = DataCleaning(column_entries=valid_categories)

    df_products = extractor.extract_from_s3(s3_address=endpoint.products)
    print(f"product rows extracted: {len(df_products.index)}")
    assert len(df_products.index) == product_config.extracted_count

    df_products = cleaner.clean_product_data(df=df_products)
    print(f"product rows after cleaning: {len(df_products.index)}")
    assert len(df_products.index) == product_config.clean_count

    target_db.upload_to_db(target_engine, df=df_products, table_name='dim_products')


def process_store_data(target_db, target_engine):
    """Extract -> Clean -> Load Store data"""
    print(f"Processing Store Data")
    extractor = DataExtractor()
    cleaner = DataCleaning(column_entries=valid_country_codes)

    headers = {
        "Content-Type": "application/json",
        "X-API-KEY": API_KEY
    }
    num_stores = extractor.list_number_of_stores(url=endpoint.number_of_stores, headers=headers)
    df_stores = extractor.retrieve_stores_data(url=endpoint.store_details, headers=headers, number_stores=num_stores)
    print(f"Store rows extracted: {len(df_stores.index)}")
    assert len(df_stores.index) == store_config.extracted_count

    df_stores = cleaner.clean_store_data(df=df_stores)
    print(f"Store rows after cleaning: {len(df_stores.index)}")
    assert len(df_stores.index) == store_config.clean_count
    target_db.upload_to_db(target_engine, df=df_stores, table_name='dim_store_details')


def process_card_data(target_db, target_engine):
    """Extract -> Clean -> Load Card data"""
    print(f"Processing Card Data")
    extractor = DataExtractor()
    cleaner = DataCleaning(column_entries=valid_card_providers)

    df_card_details = extractor.retrieve_pdf_data(pdf_path=endpoint.card_data)
    print(f"Card rows extracted: {len(df_card_details.index)}")
    assert len(df_card_details.index) == card_config.extracted_count

    df_card_details = cleaner.clean_card_data(df=df_card_details)
    print(f"Card rows after cleaning: {len(df_card_details.index)}")
    assert len(df_card_details.index) == card_config.clean_count

    target_db.upload_to_db(target_engine, df=df_card_details, table_name='dim_card_details')


def process_user_data(source_db, source_engine, target_db, target_engine):
    """Extract -> Clean -> Load User data"""
    print(f"Processing User Data")
    extractor = DataExtractor()
    cleaner = DataCleaning(column_entries=valid_country_codes)

    with source_engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
        source_db.list_db_tables(source_engine)
        df_users = extractor.read_rds_table(conn, 'legacy_users')
    print(f"User rows extracted: {len(df_users.index)}")
    assert len(df_users.index) == user_config.extracted_count

    df_users = cleaner.clean_user_data(df=df_users)
    print(f"User rows after cleaning: {len(df_users.index)}")
    assert len(df_users.index) == user_config.clean_count
    target_db.upload_to_db(target_engine, df=df_users, table_name='dim_users')


if __name__ == '__main__':
    logger.info('****************************** Starting pipeline ******************************')
    src_db, src_engine = setup_database(filename='config/db_creds.yaml')
    tgt_db, tgt_engine = setup_database(filename='config/db_creds_target.yaml')

    process_user_data(src_db, src_engine, tgt_db, tgt_engine)
    process_card_data(tgt_db, tgt_engine)
    process_store_data(tgt_db, tgt_engine)
    process_product_data(tgt_db, tgt_engine)
    process_order_data(src_engine, tgt_db, tgt_engine)
    process_date_times_data(tgt_db, tgt_engine)
