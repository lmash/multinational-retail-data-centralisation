import logging
import pandas as pd
import numpy as np
import re
from time import strptime
from typing import List

logger = logging.getLogger(__name__)


class DataCleaning:
    valid_card_providers = ['Diners Club / Carte Blanche', 'American Express', 'JCB 16 digit',
                            'JCB 15 digit', 'Maestro', 'Mastercard', 'Discover',
                            'VISA 19 digit', 'VISA 16 digit', 'VISA 13 digit']
    months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

    @staticmethod
    def _set_index_column_as_index(df) -> pd.DataFrame:
        """Set the index and sort"""
        df['index'] = df['index'].astype(np.int16)
        df = df.set_index('index')
        return df.sort_index()

    @staticmethod
    def _clean_country_code(df) -> pd.DataFrame:
        """Drop rows where country_code not valid (identifies dodgy data). Optimize after dropping"""
        logger.debug(f"Validate country_code")

        # Update Country code GGB to GB (data typo)
        df.loc[df['country_code'] == 'GGB', 'country_code'] = 'GB'

        number_rows = len(df[~df['country_code'].isin(['DE', 'GB', 'US'])].index)
        logger.debug(f"Dropping {number_rows} rows with invalid country_code")
        df = df.drop(df[~df['country_code'].isin(['DE', 'GB', 'US'])].index)

        # Change type to optimize
        df['country_code'] = df['country_code'].astype('category')
        return df

    @staticmethod
    def _clean_continent(df) -> pd.DataFrame:
        """Fix continent data with typos """
        logger.debug(f"Validate continent")
        logger.debug(f"Validate continent")
        mapping_dictionary = {'eeEurope': 'Europe', 'Europe': 'Europe', 'eeAmerica': 'America', 'America': 'America'}
        df['continent'].replace(mapping_dictionary, inplace=True)
        df['continent'] = df['continent'].astype('category')
        return df

    @staticmethod
    def _clean_staff_numbers(df) -> pd.DataFrame:
        """Fix staff numbers with non numbers """
        logger.debug(f"Validate staff_numbers")
        df['staff_numbers'] = df['staff_numbers'].apply(lambda x: re.sub("[^0-9]", "", x))
        df['staff_numbers'] = df['staff_numbers'].astype(np.uint16)
        return df

    @staticmethod
    def _drop_column_lat(df) -> pd.DataFrame:
        """Drop lat column"""
        logger.debug(f"Drop 'lat' column")
        df = df.drop('lat', axis=1)
        return df

    @staticmethod
    def _update_address(row):
        """
        Remove locality, which is after the comma. Split by line end
        The remaining entries are written to address, address_2/3/4 (where entries exist)
        """
        address = row['address']
        columns = ['address', 'address_2', 'address_3', 'address_4']

        # card addresses have the locality at the end
        if 'card_number' in row:
            locality_removed = address.split(',')[:-1]
            address = "".join(locality_removed)
        address_lines = address.split('\n')

        for line, column in zip(address_lines, columns):
            row[column] = line
            logger.debug(f"{column} set to {line}")

        return row

    def _clean_address(self, df) -> pd.DataFrame:
        """Add columns address_2, address_3, address_4 and update cleaned address"""
        logger.info(f"Clean address")
        df['address_2'] = np.nan
        df['address_3'] = np.nan
        df['address_4'] = np.nan
        df = df.apply(self._update_address, axis=1)
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

    def _clean_card_provider(self, df) -> pd.DataFrame:
        """Drop rows where card_provider not valid (identifies dodgy data)"""
        df.loc[
            ~df['card_provider'].isin(DataCleaning.valid_card_providers),
            'card_provider'
        ] = np.nan
        self._log_number_of_rows_to_drop(df=df, subset=['card_provider'])
        df = df.dropna(subset=['card_provider'])
        return df

    def _clean_card_number_expiry_date(self, df) -> pd.DataFrame:
        """
        This function uses values in 'card_number expiry_date' column to populate missing values
        in card_number and expiry_date. It also removes '???'s from card_number
        """
        df = self._set_card_number_and_expiry_date(df=df)
        df['card_number'] = df['card_number'].apply(lambda x: str(x).replace('?', ''))
        logger.debug(f"Remove ??'s from column 'card_number'")

        # Drop columns 'card_number expiry_date' and 'Unnamed'
        df = df.drop(axis=1, columns=['card_number expiry_date', 'Unnamed: 0'])
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

    def _drop_missing_data(self, df) -> pd.DataFrame:
        """NULLS have come through instead of nan - convert to nan and then delete as missing data"""
        df.loc[df['last_name'] == 'NULL', 'last_name'] = np.nan
        self._log_number_of_rows_to_drop(df=df, subset=['last_name'])
        df = df.dropna(subset=['last_name'])
        return df

    def clean_user_data(self, df):
        """
        This function cleans the user data. It removes rows with null and bad data,
        resolves errors with dates and incorrectly typed values
        """
        logger.info(f"Clean user data")
        df = self._set_index_column_as_index(df)
        df = self._drop_missing_data(df)
        df = self._clean_country_code(df)
        df = self._clean_date(df, 'date_of_birth')
        df = self._clean_date(df, 'join_date')
        df = self._clean_address(df)

        # Optimize to reduce memory
        df['country'] = df['country'].astype('category')

        return df

    def clean_card_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        This function cleans the card data. It removes erroneous values, nulls and associated bad data,
        and standardizes dates
        """
        logger.info(f"Clean card data")

        df = self._clean_card_provider(df)
        df = self._clean_date(df, 'date_payment_confirmed')
        df = self._clean_card_number_expiry_date(df)

        df.reset_index(inplace=True, drop=True)
        return df

    def clean_store_data(self, df):
        """
        This function cleans the store data. It removes rows with null and bad data,
        resolves errors with dates and incorrectly typed values
        """
        logger.info(f"Clean store data")
        df = self._set_index_column_as_index(df)
        df = self._clean_country_code(df)
        df = self._clean_continent(df)
        df = self._clean_date(df, 'opening_date')
        df = self._clean_staff_numbers(df)
        df = self._drop_column_lat(df)
        df = self._clean_address(df)
        return df

