import pandas as pd


class DataExtractor:
    def read_rds_table(self, connection, table_name) -> pd.DataFrame:
        """This function accepts an sql connection and table name and returns the table as a dataframe"""
        df = pd.read_sql_table(table_name=table_name, con=connection)
        return df
