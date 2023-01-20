import pandas as pd
import numpy as np
import os
from functools import reduce
from datetime import datetime
pd.options.mode.chained_assignment = None


def fetch_df(file, index_col=None):
    path = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__), '..', 'data', 'raw', file))
    df = pd.read_csv(path, index_col=index_col, low_memory=False)

    return df


print("Setting up...")
code_list_df = fetch_df('program_codes_list.csv', index_col=0)
general_info_df = fetch_df('financial_section_a_b_and_c_2019.csv')
id_df = fetch_df('ident_2019.csv')
financials_df = fetch_df('financial_d_and_schedule_6_2019.csv')
compensation_df = fetch_df('schedule_3_compensation_2019.csv')
web_df = fetch_df('weburl_2019.csv')


def clean_columns(df, cols):
    present_good_cols = [col for col in cols if col in df]
    return df[present_good_cols]


def apply_legal_status(x):
    return 'Active' if x == 'N' else 'Inactive'


def process_program_codes(df):
    code_list = code_list_df.to_dict()['Description']
    fields = ['Program #1 Name', 'Program #2 Name', 'Program #3 Name']
    cols = ['BN', 'FPE', 'focus_area', 'legal_status']

    for i in range(1, 4):
        df['Program #{} Name'.format(i)] = df['Program #{} Code'.format(i)].apply(lambda x: code_list.get(x))

    df['focus_area'] = df[fields].apply(lambda x: ', '.join(x.dropna().values.tolist()), axis=1)
    df['legal_status'] = df['1570'].apply(lambda x: apply_legal_status(x))

    return df[cols].set_index('BN')


def process_id_df(df):
    designation_codes = {'A': 'Public Foundation', 'B': 'Private Foundation', 'C': 'Charitable Organization'}
    province_codes = {'AB': 'Alberta', 'BC': 'British Columbia', 'MB': 'Manitoba', 'NB': 'New Brunswick', 'NL': 'Newfoundland and Labrador', 'NT': 'Northwest Territories',
                      'NS': 'Nova Scotia', 'NU': 'Nunavut', 'ON': 'Ontario', 'PE': 'Prince Edward Island', 'QC': 'Quebec', 'SK': 'Saskatchewan', 'YT': 'Yukon'}
    country_codes = {'CA': 'Canada'}

    new_df = df.assign(
        legal_designation_type=df['Designation'].apply(lambda x: designation_codes.get(x)),
        location_region=df['Province'].apply(lambda x: province_codes.get(x)),
        location_country=df['Country'].apply(lambda x: country_codes.get(x)))\
        .drop([
            'Category', 'Sub-Category', 'Designation', 'Account Name',
            'Address Line 1', 'Address Line 2',
            'Province', 'Country'], axis=1)\
        .rename(columns={
            'Legal Name': 'name',
            'Postal Code': 'location_postal_code',
            'Country': 'location_country',
            'City': 'location_municipality'
        })

    new_df[['name', 'location_municipality']] = new_df[['name', 'location_municipality']].apply(lambda x: x.str.title())

    return new_df.set_index('BN')


def process_financials_df(df):
    finance_cols = ['BN', 'revenue']

    return df\
        .rename(columns={"4700": "revenue"})\
        .pipe(clean_columns, cols=finance_cols)\
        .set_index('BN')


def process_comp_df(df):
    cols = ['BN', 'employees']
    df['employees'] = df[['300', '370']].sum(axis=1)

    return clean_columns(df, cols=cols).set_index('BN')


def process_web_df(df):
    cols = ['BN', 'Contact URL']

    new_df = df\
        .rename(columns={"BN/NE": "BN"})\
        .pipe(clean_columns, cols=cols)\
        .rename(columns={'Contact URL': 'website'})\
        .set_index('BN')

    new_df['website'] = new_df['website'].str.lower()
    return new_df


def process_scraper_output(df):
    cleaned = df[
        (df['status_code'] == 200) &
        (df['page_text'].notna()) &
        (df['partial_ratio'] > 50)].rename(columns={'page_text': 'website_text'})[['BN', 'website_text']]
    cleaned['website_text'] = cleaned['website_text'].apply(
        lambda x: x.lower()).str.replace('menu', '').str.replace('home', '')

    return cleaned


def main():
    print('Processing data...')
    scraped_df = pd.read_csv('./../webscraper/output.csv')

    df_list = [
        general_info_df.pipe(process_program_codes),
        id_df.pipe(process_id_df),
        financials_df.pipe(process_financials_df),
        compensation_df.pipe(process_comp_df),
        web_df.pipe(process_web_df),
        scraped_df.pipe(process_scraper_output)
    ]

    print('Merging data...')
    merged_df = reduce(lambda df1, df2: pd.merge(df1, df2, on='BN', how='outer'), df_list)

    print("Adding columns....")
    data_source_url = "https://open.canada.ca/data/en/dataset/d4287672-3253-4bb8-84c7-4e515ea3fddf"
    timestamp = datetime.today().strftime('%Y-%m-%d_%H:%M')
    final_df = merged_df\
        .assign(
            ent_sks_id=np.arange(start=12345, stop=(len(merged_df) + 12345)),
            regulating_authority='CRA',
            revenue_currency='CAD',
            revenue_year=2019,
            data_source=data_source_url,
            legal_status_date=2019,
            type='entity')\
        .rename(columns={'BN': 'external_id'})\
        .replace({
            "Charity provided description when other program areas are not applicable, Charity provided description when other program areas are not applicable, Charity provided description when other program areas are not applicable": "",
            "Charity provided description when other program areas are not applicable": ""
        })

    final_df.to_csv('./../data/processed/sks_entities_interface_{}.csv'.format(timestamp), index=False, header=True)
    print('Interface CSV exported')

    final_df.drop('website_text', axis=1).to_csv(
        './../data/processed/sks_entities_public_{}.csv'.format(timestamp), index=False, header=True)
    print('Public CSV exported')

    final_df\
        .drop(columns=[
            'FPE', 'focus_area', 'legal_status', 'name',
            'location_municipality', 'location_postal_code',
            'legal_designation_type', 'location_region', 'location_country',
            'revenue', 'employees', 'website', 'regulating_authority', 'revenue_currency', 'revenue_year',
            'data_source', 'legal_status_date', 'type'])\
        .to_csv('./data/processed/sks_entities_merge_{}.csv'.format(timestamp), index=False, header=True)
    print('Merge CSV exported')

    final_df\
        .drop(columns=[
            'FPE', 'legal_status', 'location_postal_code',
            'legal_designation_type', 'revenue', 'employees', 'website', 'regulating_authority', 'revenue_currency', 'revenue_year',
            'data_source', 'legal_status_date', ])\
        .to_csv('./data/processed/sks_entities_es_{}.csv'.format(timestamp), index=False, header=True)
    print('ES Upload CSV (entities) exported')


if __name__ == "__main__":
    main()
