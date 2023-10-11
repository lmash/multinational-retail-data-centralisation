import logging
import pandas as pd
import tabula

logger = logging.getLogger(__name__)


class DataExtractor:
    @staticmethod
    def read_rds_table(connection, table_name: str) -> pd.DataFrame:
        """This function accepts an sql connection and table name and returns the table as a dataframe"""
        logger.info(f"Read table {table_name} from host {connection.engine.url.host}")
        df = pd.read_sql_table(table_name=table_name, con=connection)
        return df

    @staticmethod
    def retrieve_pdf_data(pdf_path: str) -> pd.DataFrame:
        """This function accepts a path to a pdf, reads the pdf and returns the contents"""
        logger.info(f"Read pdf file from path {pdf_path}")
        df = pd.DataFrame(columns=['card_number', 'expiry_date', 'card_provider', 'date_payment_confirmed'])

        cards = tabula.read_pdf(pdf_path, stream=True)
        # cards = tabula.read_pdf(pdf_path, stream=True, pages="all")
        for card in cards:
            df = pd.concat([df, card])
        return df
