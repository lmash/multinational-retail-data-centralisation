import pandas as pd
import numpy as np
from time import strptime


class DataCleaning:
    def clean_user_data(self, df):
        """
        This function cleans the user data. It removes nulls and associated bad data,
        """
        # Set the index to be 'index'
        df = df.set_index('index')

        # Issue 1 NULLS have come through instead of na - convert to na and then delete as all respective
        # data is missing
        df.loc[df['last_name'] == 'NULL', 'last_name'] = np.nan
        df = df.dropna(subset=['last_name'])

        # Clean country code (doing this will have a knock on effect of removing some dodgy data as well)
        # 1. Update Country code GGB to GB
        df.loc[df['country_code'] == 'GGB', 'country_code'] = 'GB'
        # 2. Drop rows where country_code not valid
        df = df.drop(df[~df['country_code'].isin(['DE', 'GB', 'US'])].index)

        df = DataCleaning._fix_date(df, 'date_of_birth')
        df = DataCleaning._fix_date(df, 'join_date')

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
    def _fix_date(df, column_name):
        """Fixes date for pandas dataframe, changes made in place"""
        # Clean date_of_birth, we use newly added month column, once cleaned we can change to a datetime object
        df['month'] = df[column_name].str.slice(5, 7)
        df.loc[
            ~df['month'].isin(['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']),
            column_name
        ] = df.loc[
            ~df['month'].isin(['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']),
            column_name
        ].apply(DataCleaning._standardize_dob)
        # Drop month column
        df = df.drop('month', axis=1)

        # date_of_birth change formats which are YYYY/MM/DD to YYYY-MM-DD
        df.loc[df[column_name].str.contains('/'), column_name] = df[column_name].str.replace('/', '-')
        df.loc[:, column_name] = pd.to_datetime(df[column_name])

        return df
