import logging

from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector

logging.basicConfig(filename='pipeline.log', encoding='utf-8', level=logging.DEBUG,
                    format="%(asctime)s [%(levelname)s] %(name)s - %(funcName).40s - %(message)s",)
logger = logging.getLogger(__name__)


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

    with source_engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
        source_db.list_db_tables(source_engine)  # ['legacy_store_details', 'legacy_users', 'orders_table']
        extracted = db_extractor.read_rds_table(conn, 'legacy_users')
        extracted.info()

    df_users = cleaner.clean_user_data(df=extracted)

    # Milestone 2 Step 8 - Write users dataframe to target database
    target_db, target_engine = setup_database(filename='config/db_creds_target.yaml')
    target_db.upload_to_db(target_engine, df=df_users, table_name='dim_users')

    # Task 4 Step 2 - Extract card details to dataframe
    pdf_path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    df_card_details = db_extractor.retrieve_pdf_data(pdf_path=pdf_path)
    df_card_details = cleaner.clean_card_data(df=df_card_details)
    target_db.upload_to_db(target_engine, df=df_card_details, table_name='dim_card_details')
