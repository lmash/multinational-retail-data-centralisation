from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector


def setup_target_database():
    db_conn = DatabaseConnector()
    db_credentials = db_conn.read_db_creds('db_creds_target.yaml')
    engine = target_db.init_db_engine(db_credentials)

    return db_conn, engine


if __name__ == '__main__':
    db_connector = DatabaseConnector()
    target_db = DatabaseConnector()
    db_extractor = DataExtractor()
    cleaner = DataCleaning()
    credentials = db_connector.read_db_creds('db_creds.yaml')
    target_credentials = target_db.read_db_creds('db_creds_target.yaml')
    engine = db_connector.init_db_engine(credentials)

    with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
        db_connector.list_db_tables(engine)
#         ['legacy_store_details', 'legacy_users', 'orders_table']

        extracted = db_extractor.read_rds_table(conn, 'legacy_users')
        extracted.info()

    df = cleaner.clean_user_data(df=extracted)

    # Milestone 2 Step 8
    target_engine = target_db.init_db_engine(target_credentials)
    target_db.upload_to_db(target_engine, df=df, table_name='dim_users')
