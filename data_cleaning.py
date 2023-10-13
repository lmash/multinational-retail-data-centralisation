import logging
import pandas as pd
import numpy as np
import re
from time import strptime
from typing import List

from config import ColumnEntries, OZ_TO_KG, G_TO_KG

logger = logging.getLogger(__name__)


class DataCleaning:
    months_with_leading_zero = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

    def __init__(self, column_entries: ColumnEntries = None):
        """
        valid_or_drop is a Dictionary of column : [valid_values] entries in a column not in the list of
        valid_values will result in the row being dropped
        """
        self.valid_entries = column_entries

    @staticmethod
    def _set_index_column_as_index(df) -> pd.DataFrame:
        """Set the index and sort"""
        df['index'] = df['index'].astype(np.int32)
        df = df.set_index('index')
        return df.sort_index()

    def _clean_country_code(self, df) -> pd.DataFrame:
        """Drop rows where country_code not valid (identifies dodgy data)"""
        logger.debug(f"Clean data in column country_code")

        # Update Country code GGB to GB (data typo)
        df.loc[df['country_code'] == 'GGB', 'country_code'] = 'GB'

        df = self._drop_rows_with_invalid_entries(
            df=df,
            column=self.valid_entries.column_name,
            valid_entries=self.valid_entries.entries
        )
        return df

    @staticmethod
    def _clean_continent(df) -> pd.DataFrame:
        """Fix continent data with typos"""
        logger.debug(f"Clean data in column continent")
        mapping_dictionary = {'eeEurope': 'Europe', 'Europe': 'Europe', 'eeAmerica': 'America', 'America': 'America'}
        df['continent'].replace(mapping_dictionary, inplace=True)
        return df

    @staticmethod
    def _clean_staff_numbers(df) -> pd.DataFrame:
        """Fix staff numbers with non numbers """
        logger.debug(f"Clean data in column staff_numbers")
        df['staff_numbers'] = df['staff_numbers'].apply(lambda x: re.sub("[^0-9]", "", x))
        df['staff_numbers'] = df['staff_numbers'].astype(np.uint16)
        return df

    @staticmethod
    def _update_address(row):
        """
        Remove locality, which is after the comma. Split by line end
        The remaining entries are written to address, address_2/3/4 (where entries exist)
        """
        address = row['address']
        columns = ['address', 'address_2', 'address_3', 'address_4']

        # store addresses have the locality at the end
        if 'store_code' in row:
            locality_removed = address.split(',')[:-1]
            address = "".join(locality_removed)
        address_lines = address.split('\n')

        for line, column in zip(address_lines, columns):
            row[column] = line

        return row

    def _clean_address(self, df) -> pd.DataFrame:
        """Add columns address_2, address_3, address_4 and update cleaned address"""
        # logger.debug(f"Clean data in column address")
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
        logger.info(f"Clean data in date column {column_name}")

        # Add month column to identify dates to clean
        df['month'] = df[column_name].str.slice(5, 7)
        month_valid_mask = ~df['month'].isin(self.months_with_leading_zero)
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

    def _drop_rows_with_invalid_entries(self, df, column: str, valid_entries: List) -> pd.DataFrame:
        """
        Drop rows where entries in column are not valid (Pattern identified so far across the data,
        be very sure this is the case before using this!)
        """
        logger.debug(f"Clean data in column {column}")
        invalid_entries_mask = ~df[column].isin(valid_entries).copy()

        df.loc[
            invalid_entries_mask,
            column
        ] = np.nan

        self._log_number_of_rows_to_drop(df=df, subset=[column])
        df.dropna(subset=[column], inplace=True)
        return df

    def _clean_card_number_expiry_date(self, df) -> pd.DataFrame:
        """
        This function uses values in 'card_number expiry_date' column to populate missing values
        in card_number and expiry_date. It also removes '???'s from card_number
        """
        logger.debug(f"Clean data in columns card_number and expiry_date")
        df = self._set_card_number_and_expiry_date(df=df)
        df['card_number'] = df['card_number'].apply(lambda x: str(x).replace('?', ''))
        logger.debug(f"Remove ??'s from column 'card_number'")

        # Drop columns 'card_number expiry_date' and 'Unnamed'
        df = df.drop(axis=1, columns=['card_number expiry_date', 'Unnamed: 0'])
        df['card_number'] = df['card_number'].astype(int)
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

    @staticmethod
    def _rename_product_columns(df) -> pd.DataFrame:
        df = df.rename(columns={'Unnamed: 0': 'index'})
        return df

    @staticmethod
    def _calculate_total_weight(weight: str) -> str:
        """Accepts a weight of a number of items e.g. 2 x 200g Returns total weight in Kg"""
        number_items = weight.split(' ')
        item_weight = number_items[-1]
        total_weight = int(number_items[0]) * int(item_weight.rstrip('g'))
        return f"{total_weight / G_TO_KG}"

    @staticmethod
    def _convert_weight_from_oz_to_kg(weight: str) -> str:
        """Accepts a weight in oz e.g. 200oz Returns weight in Kg rounded to 3dp"""
        weight_in_oz = int(weight.rstrip('oz'))
        return f"{round(weight_in_oz / OZ_TO_KG, 3)}"

    @staticmethod
    def _convert_weight_from_g_or_ml_to_kg(weight: str) -> str:
        """Accepts a weight in g e.g. 200g/200ml Returns weight in Kg"""
        weight_g = float(re.sub(r'g|ml', '', weight))
        return f"{weight_g / G_TO_KG}"

    def _determine_weight_function(self, weight):
        """Routes to the appropriate function based on the contents of the weight string"""
        if 'x' in weight:
            return self._calculate_total_weight(weight)
        elif 'oz' in weight:
            return self._convert_weight_from_oz_to_kg(weight)
        elif 'kg' in weight:
            return float(weight.replace('kg', ''))

        return self._convert_weight_from_g_or_ml_to_kg(weight)

    def convert_product_weights(self, df) -> pd.DataFrame:
        """
        Converts a mixture of weight data to kg in float by first removing disallowed characters.
        The remaining data can be any of the following
          - Weights with multiple items e.g. 2 x 200g converted to 0.4
          - Weights with ml as converted 1:1 to g (converted directly to kg)
          - Weights with kg suffix remove the suffix
          - Weights with oz converted to kg
          - Weights with g converted to kg
        """
        logger.info(f"Clean data in column weight")
        df_invalid_chars = df[df['weight'].str.endswith(' .')].copy()
        df_invalid_chars['weight'] = df_invalid_chars['weight'].str.replace(' .', '')
        df.update(df_invalid_chars)

        df_rest = df.copy()
        df_rest['weight'] = df_rest['weight'].apply(self._determine_weight_function)
        df.update(df_rest)

        df['weight'] = df['weight'].astype('float')
        return df

    @staticmethod
    def _clean_product_price(df):
        """This function cleans product price by removing the £ character and converting to float"""
        logger.debug(f"Clean data in column product_price")
        price_mask = df['product_price'].str.contains('£')

        df.loc[price_mask, 'product_price'] = df.loc[price_mask, 'product_price'].str.replace('£', '')
        df['product_price'] = df['product_price'].astype('float')
        return df

    @staticmethod
    def _add_date_column(df) -> pd.DataFrame:
        """Add a date column by concatenating year, month, day and timestamp"""
        df['date'] = df['year'] + '-' + df['month'] + '-' + df['day'] + ' ' + df['timestamp']
        df['date'] = pd.to_datetime(df['date'])
        return df

    @staticmethod
    def _drop_columns(df, columns: List):
        """Drop columns first_name, last_name, 1"""
        logger.debug(f"Drop columns {columns}")
        df = df.drop(columns, axis=1)
        return df

    def clean_user_data(self, df):
        """
        This function cleans the user data. It removes rows with null and bad data,
        resolves errors with dates and incorrectly typed values
        """
        logger.info(f"Clean user data")
        df = self._set_index_column_as_index(df)
        df = self._clean_country_code(df)
        df = self._clean_date(df, 'date_of_birth')
        df = self._clean_date(df, 'join_date')
        df = self._clean_address(df)
        return df

    def clean_card_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        This function cleans the card data. It removes erroneous values, nulls and associated bad data,
        and standardizes dates
        """
        logger.info(f"Clean card data")
        df = self._drop_rows_with_invalid_entries(
            df=df,
            column=self.valid_entries.column_name,
            valid_entries=self.valid_entries.entries
        )
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
        df = self._drop_columns(df, columns=['lat'])
        df = self._clean_address(df)
        return df

    def clean_product_data(self, df):
        """
        This function cleans the product data. It removes rows with null and bad data,
        resolves errors with dates and incorrectly typed values. Its also converts all
        product weights to be in kg.
        """
        logger.info(f"Clean product data")
        df = self._rename_product_columns(df)
        df = self._set_index_column_as_index(df)
        df = self._drop_rows_with_invalid_entries(
            df=df,
            column=self.valid_entries.column_name,
            valid_entries=self.valid_entries.entries
        )
        df = self._clean_product_price(df)
        df = self._clean_date(df, 'date_added')
        df = self.convert_product_weights(df)
        return df

    def clean_order_data(self, df) -> pd.DataFrame:
        """
        This function cleans the orders data. It removes rows with null and bad data,
        resolves errors with dates and incorrectly typed values.
        """
        logger.info(f"Clean orders data")
        df = self._drop_columns(df, columns=['first_name', 'last_name', '1', 'level_0'])
        df = self._set_index_column_as_index(df)
        return df

    def clean_date_times_data(self, df) -> pd.DataFrame:
        """
        This function cleans the date_times data. It removes rows with null and bad data,
        resolves errors with dates and incorrectly typed values.
        """
        logger.info(f"Clean date_times data")
        df = self._drop_rows_with_invalid_entries(
            df=df,
            column=self.valid_entries.column_name,
            valid_entries=self.valid_entries.entries
        )
        df = self._add_date_column(df)
        return df
