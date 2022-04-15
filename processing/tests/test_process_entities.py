import pytest
import pandas as pd
import glob, os

entities = None
interface = None
merge = None

os.chdir("sks-backend/data/processed")
for file in glob.glob("*.csv"):
    if 'public' in file:
        entities = file
    elif 'merge' in file:
        merge = file
    elif 'interface' in file:
        interface = file

output_df = pd.read_csv(entities)

expected_columns = [
    'ent_sks_id',
    'external_id',
    'name',
    'focus_area',
    'legal_designation_type',
    'FPE',
    'website',
    'regulating_authority',
    'data_source',
    'location_municipality',
    'location_region',
    'location_country',
    'location_postal_code',
    'revenue',
    'revenue_year',
    'revenue_currency',
    'employees',
    'legal_status',
    'legal_status_date',
    'type'
]

def test_column_names():
    ''' Checks columns renaming worked '''

    actual = list(output_df.columns.values)
    expected = expected_columns
    assert set(actual) == set(expected)

def test_column_coverage():
    '''Checks if columns are empty'''
    acceptable_empty_cols = ['funder_id', 'actual_results']

    empty_cols = [c for c in output_df.columns.values if output_df[c].isna().all() and c not in acceptable_empty_cols]

    assert len(empty_cols) == 0

def test_cols_with_one_val():
    '''Ensure all columns expecting one value contain one value'''
    cols_with_one_val = ['location_country',
        'regulating_authority',
        'revenue_currency',
        'revenue_year',
        'data_source',
        'legal_status_date',
        'type']
    
    for c in cols_with_one_val:
        assert output_df[c].nunique() == 1

def test_column_types():
    '''Check some columns are the expected types'''
    assert output_df['ent_sks_id'].dtype == 'int64'

    for i in ['employees','revenue']:
        assert(output_df[i].dtype == 'float64')

def test_column_values():
    '''Ensure values for certain columns are expected'''
    assert output_df['legal_designation_type'].nunique() == 3

    for i in ['revenue_year', 'legal_status_date']:
        assert (output_df[i].unique() == 2019)

def test_interface_csv_contains_webtext():
    '''Ensures version of CSV for interface includes website text for search functionality'''
    interface_df = pd.read_csv(interface)
    assert 'website_text' in list(interface_df.columns.values)

def test_merge_file():
    '''Ensure the merge file only contains 2 columns required for merge'''