import pandas as pd
import pytest
import numpy as np

from config import valid_months, valid_categories, valid_country_codes, valid_card_providers
from data_cleaning import DataCleaning


@pytest.fixture
def user_cleaner():
    """Returns a DataCleaning instance for a User"""
    return DataCleaning(column_entries=valid_country_codes)


@pytest.fixture
def card_cleaner():
    """Returns a DataCleaning instance for a Card"""
    return DataCleaning(column_entries=valid_card_providers)


@pytest.fixture
def product_cleaner():
    """Returns a DataCleaning instance for a Product"""
    return DataCleaning(column_entries=valid_categories)


@pytest.fixture
def store_cleaner():
    """Returns a DataCleaning instance for a Store"""
    return DataCleaning(column_entries=valid_country_codes)


@pytest.fixture
def order_cleaner():
    """Returns a DataCleaning instance for a Store"""
    return DataCleaning()


@pytest.fixture
def date_time_cleaner():
    """Returns a DataCleaning instance for a Store"""
    return DataCleaning(column_entries=valid_months)


def test_date_of_birth_with_year_month_day(user_cleaner):
    """Test function _standardize_dob returns correct format where YYYY Month DD provided"""
    date_of_birth = '1968 October 16'
    assert user_cleaner._standardize_dob(date_of_birth) == '1968-10-16'


def test_date_of_birth_with_year_4_characters(user_cleaner):
    """
    Test function _standardize_dob returns correct format where YYYY Month DD provided
    Handles case where were initially checking the length of YYYY and a 4 character month created an issue
    """
    date_of_birth = '1968 June 16'
    assert user_cleaner._standardize_dob(date_of_birth) == '1968-06-16'


def test_date_of_birth_with_month_year_day(user_cleaner):
    """Test function _standardize_dob returns correct format where Month YYYY DD provided"""
    date_of_birth = 'October 1968 16'
    assert user_cleaner._standardize_dob(date_of_birth) == '1968-10-16'


def test_clean_date_slashes(user_cleaner):
    """Test function clean_date replaces slashes in a date with '-' and returns a pandas Timestamp"""
    df = pd.DataFrame(data=[['1972/09/09']], columns=['join_date'])
    cleaned_df = user_cleaner._clean_date(df=df, column_name='join_date')
    assert cleaned_df['join_date'][0] == pd.Timestamp('1972-09-09 00:00:00')


def test_clean_date_with_month_year_day(user_cleaner):
    """Test function clean_date replaces slashes in a date with '-' and returns a pandas Timestamp"""
    df = pd.DataFrame(data=[['July 1973 08']], columns=['join_date'])
    cleaned_df = user_cleaner._clean_date(df=df, column_name='join_date')
    assert cleaned_df['join_date'][0] == pd.Timestamp('1973-07-08 00:00:00')


def test_clean_user_data_remove_rows_with_null_in_last_name(user_cleaner):
    """Test function clean_user_data removes rows where the last_name = NULL"""
    df = pd.DataFrame(data=[
        {'index': '9',
         'last_name': 'NULL',
         'country_code': 'NULL',
         'country': 'NULL',
         'date_of_birth': 'NULL',
         'join_date': 'NULL'
         }])
    cleaned_df = user_cleaner.clean_user_data(df=df)
    assert len(cleaned_df.index) == 0


def test_clean_user_data_update_country_code_typo(user_cleaner):
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
    cleaned_df = user_cleaner.clean_user_data(df=df)
    assert cleaned_df['country_code'][7] == 'GB'


def test_clean_user_data_remove_rows_with_invalid_country_code(user_cleaner):
    """Test function clean_user_data removes rows where the country_code is not in ('GB', 'US', 'DE')"""
    df = pd.DataFrame(data=[
        ['1', 'XSHGSJH', 'XSHGSJH', 'XSHGSJH', 'XSHGSJH', 'XSHGSJH', ''],
        ['2', 'Black', 'GB', 'United Kingdom', '1999-01-01', '2021-01-01', '4 High Street\nLondon'],
        ['3', 'Red', 'DE', 'Germany', '1999-01-01', '2021-01-01', '4 Hoch Straß\nBerlin'],
        ['4', 'Green', 'US', 'United States', '1999-01-01', '2021-01-01', '4 Whatever Street\nChicago'],
    ], columns=['index', 'last_name', 'country_code', 'country', 'date_of_birth', 'join_date', 'address'])
    cleaned_df = user_cleaner.clean_user_data(df=df)
    assert all(cleaned_df['country_code'] == ['GB', 'DE', 'US']) is True


def test_set_card_number_and_expiry_date(card_cleaner):
    """Test function _set_card_number_and_expiry_date updates card_number and expiry_date"""
    df = pd.DataFrame(data=[
        {'card_number': np.nan,
         'expiry_date': np.nan,
         'card_number expiry_date': '6011036876440620 09/32'
         }])
    cleaned_df = card_cleaner._set_card_number_and_expiry_date(df=df)
    assert cleaned_df['card_number'][0] == '6011036876440620'
    assert cleaned_df['expiry_date'][0] == '09/32'


def test_card_number_and_expiry_date_unchanged(card_cleaner):
    """Test function _set_card_number_and_expiry_date leaves card_number and expiry_date unchanged"""
    df = pd.DataFrame(data=[
        {'card_number': '6011036876440620',
         'expiry_date': '09/32',
         'card_number expiry_date': np.nan
         }])
    cleaned_df = card_cleaner._set_card_number_and_expiry_date(df=df)
    assert cleaned_df['card_number'][0] == '6011036876440620'
    assert cleaned_df['expiry_date'][0] == '09/32'


def test_clean_card_data_remove_rows_with_invalid_card_provider(card_cleaner):
    """Test function clean_card_data removes rows where the card_provider is not valid"""
    df = pd.DataFrame(data=[
        ['30060773296197', '09/32', 'XSHGSJH', '2015-11-25', np.nan, np.nan],
        ['30060773296198', '09/32', 'XSHGSJH', '2015-11-25', np.nan, np.nan],
        ['30060773296198', '09/32', 'American Express', '2015-11-25', np.nan, np.nan],
    ], columns=['card_number', 'expiry_date', 'card_provider', 'date_payment_confirmed',
                'card_number expiry_date', 'Unnamed: 0'])
    cleaned_df = card_cleaner.clean_card_data(df=df)
    assert len(cleaned_df.index) == 1


def test_question_marks_removed_from_card_number(card_cleaner):
    """Test function clean_card_data removes question marks from card_provider column"""
    df = pd.DataFrame(data=[
        ['??30060773296198', '09/32', 'American Express', '2015-11-25', np.nan, np.nan],
    ], columns=['card_number', 'expiry_date', 'card_provider', 'date_payment_confirmed',
                'card_number expiry_date', 'Unnamed: 0'])
    cleaned_df = card_cleaner.clean_card_data(df=df)
    assert cleaned_df['card_number'][0] == 30060773296198


def test_typos_removed_from_store_data(store_cleaner):
    df = pd.DataFrame(data=[
        {'index': '0',
         'country_code': 'GB',
         'continent': 'eeEurope',
         'opening_date': '2015-11-25',
         'staff_numbers': '7',
         'lat': np.nan,
         'address': '4 High Street\nLondon, Greater London',
         'store_type': np.nan,
         }])
    cleaned_df = store_cleaner.clean_store_data(df=df)
    assert cleaned_df['continent'][0] == 'Europe'


def test_store_data_address_removes_locality(store_cleaner):
    df = pd.DataFrame(data=[
        {'index': '0',
         'address': 'Flat 72W\nSally isle\nEast Deantown\nE7B 8EB, High Wycombe',
         'store_code': 'HI-9B97EE4E'
         }])
    cleaned_df = store_cleaner._clean_address(df=df)
    assert cleaned_df['address'][0] == 'Flat 72W'
    assert cleaned_df['address_2'][0] == 'Sally isle'
    assert cleaned_df['address_3'][0] == 'East Deantown'
    assert cleaned_df['address_4'][0] == 'E7B 8EB'


def test_clean_product_data_removes_rows_with_invalid_category(product_cleaner):
    """Test function clean_product_data removes rows where the category not valid"""
    row_with_invalid_category = ['1']
    row_with_invalid_category.extend(['XXXXXX'] * 9)

    row_with_na = ['3']
    row_with_na.extend([np.nan]*9)

    df = pd.DataFrame(data=[
        row_with_invalid_category,
        ['2', 'Dog food', '£4.49', '12 x 100g', 'pets', '2439834307647', '1995-06-25',
         '5ec5a431-7330-4d9e-bf3c-702fc85f6efe', 'Still_avaliable', 'd4-9698287C'],
    ], columns=['index', 'product_name', 'product_price', 'weight', 'category', 'EAN', 'date_added', 'uuid',
                'removed', 'product_code'])
    cleaned_df = product_cleaner.clean_product_data(df=df)
    assert all(cleaned_df['category'].isin(valid_categories.entries)) is True


def test_convert_product_weights_with_a_number_of_items(product_cleaner):
    """Weight with a number of items in format 2 x 400g converted to Kg of total weight as a float"""
    df = pd.DataFrame(data=[
        {'index': '0',
         'weight': '2 x 200g'
         }])
    cleaned_df = product_cleaner.convert_product_weights(df=df)
    assert cleaned_df['weight'][0] == 0.4


def test_convert_product_weights_with_ml(product_cleaner):
    """Weight in ml converted to Kg as a float"""
    df = pd.DataFrame(data=[
        {'index': '0',
         'weight': '600ml'
         }])
    cleaned_df = product_cleaner.convert_product_weights(df=df)
    assert cleaned_df['weight'][0] == 0.6


def test_convert_product_weights_with_kg(product_cleaner):
    """Weight in kg has suffix removed as a float"""
    df = pd.DataFrame(data=[
        {'index': '0',
         'weight': '0.6kg'
         }])
    cleaned_df = product_cleaner.convert_product_weights(df=df)
    assert cleaned_df['weight'][0] == 0.6


def test_convert_product_weights_with_oz(product_cleaner):
    """Weight in oz converted to kg as a float"""
    df = pd.DataFrame(data=[
        {'index': '0',
         'weight': '16oz'
         }])
    cleaned_df = product_cleaner.convert_product_weights(df=df)
    assert cleaned_df['weight'][0] == pytest.approx(0.4536, rel=0.001)


def test_product_weight_remove_invalid_characters(product_cleaner):
    """Weights ending in ' .' are cleaned and converted to kg as a float"""
    df = pd.DataFrame(data=[
        {'index': '0',
         'weight': '16g .'
         }])
    cleaned_df = product_cleaner.convert_product_weights(df=df)
    assert cleaned_df['weight'][0] == 0.016


def test_convert_product_weights_with_g(product_cleaner):
    """Weight in g converted to kg as a float"""
    df = pd.DataFrame(data=[
        {'index': '0',
         'weight': '11.6g'
         }])
    cleaned_df = product_cleaner.convert_product_weights(df=df)
    assert cleaned_df['weight'][0] == 0.0116


def test_drop_columns(order_cleaner):
    """Columns are dropped"""
    df = pd.DataFrame(data=[
        {'index': '0',
         'product_quantity': '3',
         'first_name': 'NULL',
         'last_name': 'NULL',
         }])
    cleaned_df = order_cleaner._drop_columns(df, columns=['first_name', 'last_name'])
    assert 'first_name' not in cleaned_df.columns
    assert 'last_name' not in cleaned_df.columns
    assert 'index' in cleaned_df.columns


def test_add_date_column(date_time_cleaner):
    """New column created which concatenates year, month, day and timestamp"""
    df = pd.DataFrame(data=[
        {'timestamp': '22:00:06',
         'month': '3',
         'year': '1972',
         'day': '28',
         }])
    cleaned_df = date_time_cleaner._add_date_column(df)
    assert cleaned_df['date'][0] == pd.Timestamp('1972-03-28 22:00:06')
