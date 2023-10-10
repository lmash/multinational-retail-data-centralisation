import logging

from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector

logging.basicConfig(filename='pipeline.log', encoding='utf-8', level=logging.DEBUG,
                    format="%(asctime)s [%(levelname)s] %(name)s - %(funcName).20s - %(message)s",)
logger = logging.getLogger(__name__)


def setup_database(filename):
    db_conn = DatabaseConnector()
    db_credentials = db_conn.read_db_creds(filename)
    engine = db_conn.init_db_engine(db_credentials)
    return db_conn, engine


if __name__ == '__main__':
    db_extractor = DataExtractor()
    cleaner = DataCleaning()
    source_db, source_engine = setup_database(filename='config/db_creds.yaml')

    with source_engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
        source_db.list_db_tables(source_engine)  # ['legacy_store_details', 'legacy_users', 'orders_table']
        extracted = db_extractor.read_rds_table(conn, 'legacy_users')
        extracted.info()

    df = cleaner.clean_user_data(df=extracted)

    # # Milestone 2 Step 8
    target_db, target_engine = setup_database(filename='config/db_creds_target.yaml')
    target_db.upload_to_db(target_engine, df=df, table_name='dim_users')
