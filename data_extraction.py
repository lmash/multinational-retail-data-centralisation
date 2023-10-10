import logging
import pandas as pd

logger = logging.getLogger(__name__)


class DataExtractor:
    def read_rds_table(self, connection, table_name) -> pd.DataFrame:
        """This function accepts an sql connection and table name and returns the table as a dataframe"""
        logger.info(f"Read table {table_name} from host {connection.engine.url.host}")
        df = pd.read_sql_table(table_name=table_name, con=connection)
        return df
