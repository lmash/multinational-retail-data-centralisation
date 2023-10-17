import logging
from sqlalchemy import create_engine, inspect
from typing import Dict
import yaml

logger = logging.getLogger(__name__)


class DatabaseConnector:

    @staticmethod
    def read_db_creds(filename: str) -> Dict:
        """This function accepts a filename (with a path) and returns the database credentials"""
        logger.info(f"Read database credentials from YAML file: {filename}")
        with open(filename, mode='r') as f:
            db_credentials = yaml.safe_load(f)
        return db_credentials

    @staticmethod
    def init_db_engine(credentials: Dict):
        """This function accepts database credentials and returns an SQL alchemy engine"""
        logger.info(f"Initialize db engine for: {credentials['RDS_HOST']}")
        db_type = "postgresql+psycopg2"
        engine = create_engine(
            f"{db_type}://{credentials['RDS_USER']}:{credentials['RDS_PASSWORD']}"
            f"@{credentials['RDS_HOST']}:{credentials['RDS_PORT']}/{credentials['RDS_DATABASE']}"
        )
        return engine

    @staticmethod
    def list_db_tables(engine):
        """This function accepts an SQL alchemy engine and prints tables in the public schema"""
        logger.info(f"List Database tables for: {engine}")
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        logger.info(f"Tables: {table_names}")

    @staticmethod
    def upload_to_db(engine, df, table_name):
        """This function accepts a dataframe and uploads it to the table_name"""
        logger.info(f"Upload dataframe to: {table_name}")
        df.to_sql(table_name, engine, if_exists='replace')
