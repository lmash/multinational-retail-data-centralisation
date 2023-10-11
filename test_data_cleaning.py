import pandas as pd
import numpy as np

from data_cleaning import DataCleaning


def test_date_of_birth_with_year_month_day():
    """Test function _standardize_dob returns correct format where YYYY Month DD provided"""
    date_of_birth = '1968 October 16'
    assert DataCleaning._standardize_dob(date_of_birth) == '1968-10-16'


def test_date_of_birth_with_year_4_characters():
    """
    Test function _standardize_dob returns correct format where YYYY Month DD provided
    Handles case where were initially checking the length of YYYY and a 4 character month created an issue
    """
    date_of_birth = '1968 June 16'
    assert DataCleaning._standardize_dob(date_of_birth) == '1968-06-16'


def test_date_of_birth_with_month_year_day():
    """Test function _standardize_dob returns correct format where Month YYYY DD provided"""
    date_of_birth = 'October 1968 16'
    assert DataCleaning._standardize_dob(date_of_birth) == '1968-10-16'


def test_clean_user_date_slashes():
    """Test function clean_user_date replaces slashes in a date with '-' and returns a pandas Timestamp"""
    df = pd.DataFrame(data=[['1972/09/09']], columns=['join_date'])
    cleaning = DataCleaning()
    cleaned_df = cleaning._clean_date(df=df, column_name='join_date')
    assert cleaned_df['join_date'][0] == pd.Timestamp('1972-09-09 00:00:00')


def test_clean_user_date_with_month_year_day():
    """Test function clean_user_date replaces slashes in a date with '-' and returns a pandas Timestamp"""
    df = pd.DataFrame(data=[['July 1973 08']], columns=['join_date'])
    cleaning = DataCleaning()
    cleaned_df = cleaning._clean_date(df=df, column_name='join_date')
    assert cleaned_df['join_date'][0] == pd.Timestamp('1973-07-08 00:00:00')


def test_clean_user_data_remove_rows_with_null_in_last_name():
    """Test function clean_user_data removes rows where the last_name = NULL"""
    df = pd.DataFrame(data=[
        {'index': '9',
         'last_name': 'NULL',
         'country_code': 'NULL',
         'country': 'NULL',
         'date_of_birth': 'NULL',
         'join_date': 'NULL'
         }])
    cleaning = DataCleaning()
    cleaned_df = cleaning.clean_user_data(df=df)
    assert len(cleaned_df.index) == 0


def test_clean_user_data_update_country_code_typo():
    """Test function clean_user_data updates country_code to GB where it is GGB"""
    df = pd.DataFrame(data=[
        {'index': '7',
         'last_name': 'Smith',
         'country_code': 'GGB',
         'country': 'United Kingdom',
         'date_of_birth': 'July 1981 07',
         'join_date': '2021 June 30',
         'address': '4 High Street\nLondon'
         }])
    cleaning = DataCleaning()
    cleaned_df = cleaning.clean_user_data(df=df)
    assert cleaned_df['country_code'][7] == 'GB'


def test_clean_user_data_remove_rows_with_invalid_country_code():
    """Test function clean_user_data removes rows where the country_code is not in ('GB', 'US', 'DE')"""
    df = pd.DataFrame(data=[
        ['1', 'XSHGSJH', 'XSHGSJH', 'XSHGSJH', 'XSHGSJH', 'XSHGSJH', ''],
        ['2', 'Black', 'GB', 'United Kingdom', '1999-01-01', '2021-01-01', '4 High Street\nLondon'],
        ['3', 'Red', 'DE', 'Germany', '1999-01-01', '2021-01-01', '4 Hoch Straß\nBerlin'],
        ['4', 'Green', 'US', 'United States', '1999-01-01', '2021-01-01', '4 Whatever Street\nChicago'],
    ], columns=['index', 'last_name', 'country_code', 'country', 'date_of_birth', 'join_date', 'address'])
    cleaning = DataCleaning()
    cleaned_df = cleaning.clean_user_data(df=df)
    assert all(cleaned_df['country_code'] == ['GB', 'DE', 'US']) is True


def test_set_card_number_and_expiry_date():
    """Test function _set_card_number_and_expiry_date updates card_number and expiry_date"""
    df = pd.DataFrame(data=[
        {'card_number': np.nan,
         'expiry_date': np.nan,
         'card_number expiry_date': '6011036876440620 09/32'
         }])
    cleaning = DataCleaning()
    cleaned_df = cleaning._set_card_number_and_expiry_date(df=df)
    assert cleaned_df['card_number'][0] == '6011036876440620'
    assert cleaned_df['expiry_date'][0] == '09/32'


def test_card_number_and_expiry_date_unchanged():
    """Test function _set_card_number_and_expiry_date leaves card_number and expiry_date unchanged"""
    df = pd.DataFrame(data=[
        {'card_number': '6011036876440620',
         'expiry_date': '09/32',
         'card_number expiry_date': np.nan
         }])
    cleaning = DataCleaning()
    cleaned_df = cleaning._set_card_number_and_expiry_date(df=df)
    assert cleaned_df['card_number'][0] == '6011036876440620'
    assert cleaned_df['expiry_date'][0] == '09/32'


def test_clean_card_data_remove_rows_with_invalid_card_provider():
    """Test function clean_card_data removes rows where the card_provider is not valid"""
    df = pd.DataFrame(data=[
        ['30060773296197', '09/32', 'XSHGSJH', '2015-11-25', np.nan, np.nan],
        ['30060773296198', '09/32', 'XSHGSJH', '2015-11-25', np.nan, np.nan],
        ['30060773296198', '09/32', 'American Express', '2015-11-25', np.nan, np.nan],
    ], columns=['card_number', 'expiry_date', 'card_provider', 'date_payment_confirmed',
                'card_number expiry_date', 'Unnamed: 0'])
    cleaning = DataCleaning()
    cleaned_df = cleaning.clean_card_data(df=df)
    assert len(cleaned_df.index) == 1


def test_question_marks_removed_from_card_number():
    """Test function clean_card_data removes question marks from card_provider column"""
    df = pd.DataFrame(data=[
        ['??30060773296198', '09/32', 'American Express', '2015-11-25', np.nan, np.nan],
    ], columns=['card_number', 'expiry_date', 'card_provider', 'date_payment_confirmed',
                'card_number expiry_date', 'Unnamed: 0'])
    cleaning = DataCleaning()
    cleaned_df = cleaning.clean_card_data(df=df)
    assert cleaned_df['card_number'][0] == '30060773296198'


def test_typos_removed_from_store_data():
    df = pd.DataFrame(data=[
        {'index': '0',
         'country_code': 'GB',
         'continent': 'eeEurope',
         'opening_date': '2015-11-25',
         'staff_numbers': '7',
         'lat': np.nan,
         'address': '4 High Street\nLondon, Greater London'
         }])
    cleaning = DataCleaning()
    cleaned_df = cleaning.clean_store_data(df=df)
    assert cleaned_df['continent'][0] == 'Europe'
