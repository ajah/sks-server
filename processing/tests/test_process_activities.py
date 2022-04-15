import pytest
import pandas as pd
import glob, os

activities = None

os.chdir("sks-backend/data/processed")
for file in glob.glob("*.csv"):
    if 'activities' in file:
        activities = file

output_df = pd.read_csv(activities)

expected_columns = [ 
        'act_sks_id',
        'source_id',
        'source_authority',
        'source_url',
        'grant_title',
        'funding_amount',
        'funding_type',
        'funder',
        'recipient_organization',
        'recipient_id',
        'grant_description',
        'funder_id',
        'grant_region',
        'grant_municipality',
        'date',
        'date_type',
        'end_date',
        'end_date_type',
        'expected_results',
        # 'actual_results',
        'program_name',
        'type',
        'ent_sks_id'
    ]


def test_column_names():
    ''' Checks columns renaming worked '''

    actual = output_df.columns.values
    expected = expected_columns
    assert set(actual) == set(expected)

def test_column_coverage():
    '''Checks if columns are empty'''
    acceptable_empty_cols = ['funder_id', 'actual_results']

    empty_cols = [c for c in output_df.columns.values if output_df[c].isna().all() and c not in acceptable_empty_cols]

    assert len(empty_cols) == 0


def test_municipalities_contain_pipes():
    assert output_df['grant_municipality'].str.contains('\|').any() == False

def test_grant_titles():
    '''After cascading matches, no grant titles should be blank'''
    assert output_df['grant_title'].isna().any()

def test_cols_with_one_val():
    cols_with_one_val = [
        'source_authority',
        'source_url',
        'funding_type',
        'date_type',
        'end_date_type',
        'type'
    ]

    for c in cols_with_one_val:
        assert output_df[c].nunique() == 1

def test_column_types():
    '''Funding amoutn should be a float'''
    assert output_df['funding_amount'].dtype == 'float64'

def test_ent_npk_id():
    '''Ensure ent_npk_exists and is not null'''
    assert output_df['ent_sks_id'].isna().all() == False

def test_province_names():
    '''Tests whether province renaming works'''
    provinces = [
        'Alberta',
        'British Columbia',
        'Manitoba', 
        'New Brunswick',
        'Newfoundland and Labrador',
        'Northwest Territories', 
        'Nova Scotia',
        'Nunavut', 
        'Ontario', 
        'Prince Edward Island', 
        'Quebec', 
        'Saskatchewan',
        'Yukon' ]
    
    assert output_df['grant_region'].isin(provinces).any() == True

def test_dates():
    '''Ensure start & end dates are in the same format ahead of date mapping'''
    regex = '^\d{4}\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01])$'
    assert output_df['date'].str.match(regex).all() == True