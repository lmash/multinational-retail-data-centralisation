from dotenv import load_dotenv
import logging
import os

from config import endpoints, card, user, store, order, product, date_times, DataType
from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector

logging.basicConfig(filename='pipeline.log', encoding='utf-8', level=logging.DEBUG,
                    format="%(asctime)s [%(levelname)s] %(name)s - %(funcName).40s - %(message)s",)
logger = logging.getLogger(__name__)
load_dotenv()


def setup_database(filename):
    db_conn = DatabaseConnector()
    db_credentials = db_conn.read_db_creds(filename)
    engine = db_conn.init_db_engine(db_credentials)
    return db_conn, engine


def process_date_times_data(target_db, target_engine, data_type: DataType):
    """Extract -> Clean -> Load Product data"""
    print(f"Processing {data_type.name} Data")

    # Extract
    data_extractor = DataExtractor()
    df_extracted = data_extractor.extract_from_s3(s3_address=endpoints.date_times)
    print(f"{data_type.name} rows extracted: {len(df_extracted.index)}")
    assert len(df_extracted.index) == data_type.extracted_count

    # Clean
    data_cleaner = DataCleaning(column_entries=data_type.column_entries)
    df_cleaned = df_extracted.copy()
    df_cleaned = data_cleaner.clean_date_times_data(df=df_cleaned)
    print(f"{data_type.name} rows after cleaning: {len(df_cleaned.index)}")
    assert len(df_cleaned.index) == data_type.clean_count

    # Load
    target_db.upload_to_db(target_engine, df=df_cleaned, table_name=data_type.table_name)


def process_order_data(source_engine, target_db, target_engine, data_type: DataType):
    """Extract -> Clean -> Load Order data"""
    print(f"Processing {data_type.name} Data")

    # Extract
    data_extractor = DataExtractor()
    with source_engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
        df_extracted = data_extractor.read_rds_table(conn, 'orders_table')
    print(f"{data_type.name} rows extracted: {len(df_extracted.index)}")
    assert len(df_extracted.index) == data_type.extracted_count

    # Clean
    data_cleaner = DataCleaning(column_entries=data_type.column_entries)
    df_cleaned = df_extracted.copy()
    df_cleaned = data_cleaner.clean_order_data(df=df_cleaned)
    print(f"{data_type.name} rows after cleaning: {len(df_cleaned.index)}")
    assert len(df_cleaned.index) == data_type.clean_count

    # Load
    target_db.upload_to_db(target_engine, df=df_cleaned, table_name=data_type.table_name)


def process_product_data(target_db, target_engine, data_type: DataType):
    """Extract -> Clean -> Load Product data"""
    print(f"Processing {data_type.name} Data")

    # Extract
    data_extractor = DataExtractor()
    df_extracted = data_extractor.extract_from_s3(s3_address=endpoints.products)
    print(f"{data_type.name} rows extracted: {len(df_extracted.index)}")
    assert len(df_extracted.index) == data_type.extracted_count

    # Clean
    data_cleaner = DataCleaning(column_entries=data_type.column_entries)
    df_cleaned = df_extracted.copy()
    df_cleaned = data_cleaner.clean_product_data(df=df_cleaned)
    print(f"{data_type.name} rows after cleaning: {len(df_cleaned.index)}")
    assert len(df_cleaned.index) == data_type.clean_count

    # Load
    target_db.upload_to_db(target_engine, df=df_cleaned, table_name=data_type.table_name)


def process_store_data(target_db, target_engine, data_type: DataType):
    """Extract -> Clean -> Load Store data"""
    print(f"Processing {data_type.name} Data")
    headers = {
        "Content-Type": "application/json",
        "X-API-KEY": os.getenv('x-api-key')
    }

    # Extract
    data_extractor = DataExtractor()
    num_stores = data_extractor.list_number_of_stores(url=endpoints.number_of_stores, headers=headers)
    df_extracted = data_extractor.retrieve_stores_data(
        url=endpoints.store_details, headers=headers, number_stores=num_stores)
    print(f"{data_type.name} rows extracted: {len(df_extracted.index)}")
    assert len(df_extracted.index) == data_type.extracted_count

    # Clean
    data_cleaner = DataCleaning(column_entries=data_type.column_entries)
    df_cleaned = df_extracted.copy()
    df_cleaned = data_cleaner.clean_store_data(df=df_cleaned)
    print(f"{data_type.name} rows after cleaning: {len(df_cleaned.index)}")
    assert len(df_cleaned.index) == data_type.clean_count

    # Load
    target_db.upload_to_db(target_engine, df=df_cleaned, table_name=data_type.table_name)


def process_card_data(target_db, target_engine, data_type: DataType):
    """Extract from a pdf file -> Clean -> Load Card data"""
    print(f"Processing {data_type.name} Data")

    # Extract
    data_extractor = DataExtractor()
    df_extracted = data_extractor.retrieve_pdf_data(pdf_path=endpoints.card_data)
    print(f"{data_type.name} rows extracted: {len(df_extracted.index)}")
    assert len(df_extracted.index) == data_type.extracted_count

    # Clean
    data_cleaner = DataCleaning(column_entries=data_type.column_entries)
    df_cleaned = df_extracted.copy()
    df_cleaned = data_cleaner.clean_card_data(df=df_cleaned)
    print(f"{data_type.name} rows after cleaning: {len(df_cleaned.index)}")
    assert len(df_cleaned.index) == data_type.clean_count

    # Load
    target_db.upload_to_db(target_engine, df=df_cleaned, table_name=data_type.table_name)


def process_user_data(source_db, source_engine, target_db, target_engine, data_type: DataType):
    """Extract from a postgres Database -> Clean -> Load User data"""
    print(f"Processing {data_type.name} Data")

    data_extractor = DataExtractor()
    with source_engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
        source_db.list_db_tables(source_engine)
        df_extracted = data_extractor.read_rds_table(conn, 'legacy_users')
    print(f"{data_type.name} rows extracted: {len(df_extracted.index)}")
    assert len(df_extracted.index) == data_type.extracted_count

    data_cleaner = DataCleaning(column_entries=data_type.column_entries)
    df_cleaned = df_extracted.copy()
    df_cleaned = data_cleaner.clean_user_data(df=df_cleaned)
    print(f"{data_type.name} rows after cleaning: {len(df_cleaned.index)}")
    assert len(df_cleaned.index) == data_type.clean_count

    target_db.upload_to_db(target_engine, df=df_cleaned, table_name=data_type.table_name)


if __name__ == '__main__':
    logger.info('****************************** Starting pipeline ******************************')
    src_db, src_engine = setup_database(filename='config/db_creds.yaml')
    tgt_db, tgt_engine = setup_database(filename='config/db_creds_target.yaml')

    # process_user_data(src_db, src_engine, tgt_db, tgt_engine, data_type=user)
    # process_card_data(tgt_db, tgt_engine, data_type=card)
    # process_store_data(tgt_db, tgt_engine, data_type=store)
    process_product_data(tgt_db, tgt_engine, data_type=product)
    # process_order_data(src_engine, tgt_db, tgt_engine, data_type=order)
    # process_date_times_data(tgt_db, tgt_engine, data_type=date_times)
