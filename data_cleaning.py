import logging
import pandas as pd
import numpy as np
from time import strptime

logger = logging.getLogger(__name__)


class DataCleaning:
    def clean_user_data(self, df):
        """
        This function cleans the user data. It removes nulls and associated bad data,
        """
        logger.info(f"Clean user data")
        # Set the index and sort
        df['index'] = df['index'].astype(np.int16)
        df = df.set_index('index')
        df = df.sort_index()

        # NULLS have come through instead of nan - convert to nan and then delete as missing data
        df.loc[df['last_name'] == 'NULL', 'last_name'] = np.nan
        df = df.dropna(subset=['last_name'])

        # Update Country code GGB to GB (data typo)
        df.loc[df['country_code'] == 'GGB', 'country_code'] = 'GB'
        # Drop rows where country_code not valid (identifies dodgy data)
        df = df.drop(df[~df['country_code'].isin(['DE', 'GB', 'US'])].index)

        df = DataCleaning._clean_user_date(df, 'date_of_birth')
        df = DataCleaning._clean_user_date(df, 'join_date')

        # Optimize to reduce memory
        df['country'] = df['country'].astype('category')
        df['country_code'] = df['country_code'].astype('category')

        return df

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

    @staticmethod
    def _clean_user_date(df, column_name) -> pd.DataFrame:
        """
        Clean user date in pandas dataframe, 4 date formats are used
          -- YYYY-MM-DD
          -- YYYY Month DD
          -- Month YYYY DD
          -- YYYY/MM/DD
        returns the dataframe with the date standardised to YYYY-MM-DD in pandas Timestamp format
        """
        # Add month column to identify dates to clean
        df['month'] = df[column_name].str.slice(5, 7)

        df.loc[
            ~df['month'].isin(['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']),
            column_name
        ] = df.loc[
            ~df['month'].isin(['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']),
            column_name
        ].apply(DataCleaning._standardize_dob)
        df = df.drop('month', axis=1)

        # date_of_birth change formats which are YYYY/MM/DD to YYYY-MM-DD
        df.loc[df[column_name].str.contains('/'), column_name] = df[column_name].str.replace('/', '-')
        df[column_name] = pd.to_datetime(df[column_name])

        return df
