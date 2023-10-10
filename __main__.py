from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector

if __name__ == '__main__':
    db_connector = DatabaseConnector()
    db_extractor = DataExtractor()
    cleaner = DataCleaning()
    credentials = db_connector.read_db_creds('db_creds.yaml')
    engine = db_connector.init_db_engine(credentials)

    with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
        # Milestone 2 Step 4
        db_connector.list_db_tables(engine)
#         ['legacy_store_details', 'legacy_users', 'orders_table']

#         Milestone 2 Step 5
        extracted = db_extractor.read_rds_table(conn, 'legacy_users')
        extracted.info()

    # Milestone 2 Step 6
    df = cleaner.clean_user_data(df=extracted)
