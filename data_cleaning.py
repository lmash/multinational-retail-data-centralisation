import logging
import pandas as pd
import numpy as np
from time import strptime
from typing import List

logger = logging.getLogger(__name__)


class DataCleaning:
    valid_card_providers = ['Diners Club / Carte Blanche', 'American Express', 'JCB 16 digit',
                            'JCB 15 digit', 'Maestro', 'Mastercard', 'Discover',
                            'VISA 19 digit', 'VISA 16 digit', 'VISA 13 digit']
    months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

    def clean_user_data(self, df):
        """
        This function cleans the user data. It removes rows with null and bad data,
        resolves errors with dates and incorrectly typed values
        """
        logger.info(f"Clean user data")
        # Set the index and sort
        df['index'] = df['index'].astype(np.int16)
        df = df.set_index('index')
        df = df.sort_index()

        # NULLS have come through instead of nan - convert to nan and then delete as missing data
        df.loc[df['last_name'] == 'NULL', 'last_name'] = np.nan
        self._log_number_of_rows_to_drop(df=df, subset=['last_name'])
        df = df.dropna(subset=['last_name'])

        # Update Country code GGB to GB (data typo)
        df.loc[df['country_code'] == 'GGB', 'country_code'] = 'GB'
        # Drop rows where country_code not valid (identifies dodgy data)
        df = df.drop(df[~df['country_code'].isin(['DE', 'GB', 'US'])].index)

        df = self._clean_date(df, 'date_of_birth')
        df = self._clean_date(df, 'join_date')

        # Optimize to reduce memory
        df['country'] = df['country'].astype('category')
        df['country_code'] = df['country_code'].astype('category')

        return df

    @staticmethod
    def _log_number_of_rows_to_drop(df, subset: List):
        """Return number of rows which will be dropped"""
        number_rows = len(df.index) - len(df.dropna(subset=subset))
        logger.debug(f"Dropping {number_rows} rows for subset {subset}")

    @staticmethod
    def _standardize_dob(date) -> str:
        """The month and years are in varying orders, always return as YYYY-MM-DD"""
        date_split = date.split()

        if date_split[0].isnumeric():
            # Handle YYYY Month Day e.g. 1968 October 16
            month = strptime(date_split[1], '%B').tm_mon
            return f"{date_split[0]}-{month:02d}-{date_split[2]}"

        # Handle Month YYYY Day e.g. January 1951 27
        month = strptime(date_split[0], '%B').tm_mon
        return f"{date_split[1]}-{month:02d}-{date_split[2]}"

    def _clean_date(self, df, column_name) -> pd.DataFrame:
        """
        Clean user date in pandas dataframe, 4 date formats are used
          -- YYYY-MM-DD
          -- YYYY Month DD
          -- Month YYYY DD
          -- YYYY/MM/DD
        returns the dataframe with the date standardised to YYYY-MM-DD in pandas Timestamp format
        """
        logger.info(f"Clean date {column_name}")

        # Add month column to identify dates to clean
        df['month'] = df[column_name].str.slice(5, 7)
        month_valid_mask = ~df['month'].isin(self.months)
        df.loc[
            month_valid_mask,
            column_name
        ] = df.loc[
            month_valid_mask,
            column_name
        ].apply(DataCleaning._standardize_dob)
        df = df.drop('month', axis=1)

        # Change formats which are YYYY/MM/DD to YYYY-MM-DD
        df[column_name] = df[column_name].apply(lambda x: x.replace('/', '-'))
        df[column_name] = pd.to_datetime(df[column_name])

        return df

    def clean_card_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        This function cleans the card data. It removes erroneous values, nulls and associated bad data,
        and standardizes dates
        """
        logger.info(f"Clean card data")

        # Drop rows where card_provider not valid (identifies dodgy data)
        df.loc[
            ~df['card_provider'].isin(DataCleaning.valid_card_providers),
            'card_provider'
        ] = np.nan
        self._log_number_of_rows_to_drop(df=df, subset=['card_provider'])
        df = df.dropna(subset=['card_provider'])

        df = self._clean_date(df, 'date_payment_confirmed')
        df = self._set_card_number_and_expiry_date(df=df)

        # Drop columns 'card_number expiry_date' and 'Unnamed'
        df = df.drop(axis=1, columns=['card_number expiry_date', 'Unnamed: 0'])

        # Remove the question marks in card_number
        df['card_number'] = df['card_number'].apply(lambda x: str(x).replace('?', ''))
        logger.debug(f"Remove ??'s from column 'card_number'")

        df.reset_index(inplace=True, drop=True)
        return df

    @staticmethod
    def _set_card_number_and_expiry_date(df) -> pd.DataFrame:
        """Copies values from column 'card_number expiry_date' to card_number and expiry_date"""
        logger.debug(f"Set the card number and expiry dates for nan entries from 'card_number expiry_date'")
        card_no_expiry_date_mask = ~df['card_number expiry_date'].isna()

        df.loc[
            card_no_expiry_date_mask,
            'card_number'
        ] = df.loc[
            card_no_expiry_date_mask,
            'card_number expiry_date'
        ].apply(lambda x: x.split()[0])

        df.loc[
            card_no_expiry_date_mask,
            'expiry_date'
        ] = df.loc[
            card_no_expiry_date_mask,
            'card_number expiry_date'
        ].apply(lambda x: x.split()[1])

        return df

