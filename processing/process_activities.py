import pandas as pd
from flask import jsonify
import json
from datetime import datetime
import re 
import traceback
import numpy as np 
import glob, os
pd.options.mode.chained_assignment = None 

tables_path = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..','data', 'raw','full_proactive_disclosure_dataset.csv'))

entities_merge = None

os.chdir("sks-backend/data/processed")
for file in glob.glob("*.csv"):
    if 'merge' in file:
        entities_merge = file

print("Setting up...")

def get_npk_id(number):
  entries = []
  base = 12345
  for i in range(0, number):
    if len(entries) == 0:
      entries.append(base)
    else:
      entries.append(entries[-1] +1)
  return entries

def get_entries(df):
    return df.shape[0]

def remove_pipes_commas(x):
    return x.split("|")[0].split(",")[0]

def clean_columns(df):
    required_cols = [
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
        'ent_sks_id',
        'actual_results',
        'program_name',
        'type'
    ]

    present_good_cols = [col for col in required_cols if col in df]
    missing_good_cols = list(set(required_cols) - set(present_good_cols))

    not_renamed_cols = [col for col in df.columns if re.match(r'^Q\d+$', col)]
    if missing_good_cols or not_renamed_cols:
        # Check for missing
        if missing_good_cols:
            print("Dataframe does not contain \
                {missing_good_cols}".format(
                missing_good_cols=missing_good_cols))
        if not_renamed_cols:
            print("Dataframe still contains un-renamed \
                question columns: {not_renamed_cols}".format(
                not_renamed_cols=not_renamed_cols))

    assert len(present_good_cols) == len(
        set(present_good_cols)), "No duplicate cols"

    return df[present_good_cols]

def cascade_fill(x):
    # Note: the sub-df for this function is cast to strings, so all operations are string-based
    if x['agreement_title_en'].isdigit() or x['agreement_title_en'] == 'nan':
        if x['prog_name_en'] != 'nan' and x['recipient_legal_name'] != 'nan':
            return x['prog_name_en'] + " - " + x['recipient_legal_name'] 
        elif x['prog_name_en'] == 'nan' and x['recipient_legal_name'] != 'nan':
            return x['recipient_legal_name']  
    else:
        return x['agreement_title_en']

def main():
    print('Reading data...')
    df = pd.read_csv(tables_path,low_memory=False)

    print('Processing data...')
    timestamp = datetime.today().strftime('%Y-%m-%d_%H:%M')
    ent_npk_id = pd.read_csv(entities_merge)
    province_codes = { 'AB': 'Alberta', 'BC': 'British Columbia', 'MB': 'Manitoba', 'NB': 'New Brunswick', 'NL': 'Newfoundland and Labrador', 'NT': 'Northwest Territories', 'NS': 'Nova Scotia', 'NU': 'Nunavut', 'ON': 'Ontario', 'PE': 'Prince Edward Island', 'QC': 'Quebec', 'SK': 'Saskatchewan', 'YT': 'Yukon' }

    try:
        # Column operations migrated here for performance reasons
        df['grant_title'] = df[['agreement_title_en','prog_name_en','recipient_legal_name']].astype(str).apply(lambda x: cascade_fill(x), axis=1).apply(lambda x: remove_pipes_commas(str(x)))
        df['grant_municipality']=df['recipient_city'].apply(lambda x: remove_pipes_commas(str(x)))
        df['grant_region'] = df['recipient_province'].apply(lambda x: province_codes.get(x))
        df['recipient_organization'] = df['recipient_legal_name'].apply(lambda x: remove_pipes_commas(str(x)))

        df = df\
            .replace(r'\n',' ', regex=True)\
            .assign(
                act_sks_id=np.arange(start=12345, stop=(len(df)+12345)),
                source_authority = 'Proactive Disclosure - Grants and Contributions',
                source_url = 'https://open.canada.ca/data/en/dataset/432527ab-7aac-45b5-81d6-7597107a7013',
                funding_type = 'Amount Applied For',
                funder_id = '', # to be filled on a per-row basis
                date_type = 'Grant Start Date',
                end_date_type= 'Grant End Date',
                type='activity',
            )\
            .rename(
                columns={
                    'ref_number': 'source_id', 
                    'agreement_value':'funding_amount', 
                    'owner_org':'funder',
                    'recipient_business_number':'recipient_id',
                    'description_en':'grant_description',
                    'agreement_start_date':'date',
                    'agreement_end_date':'end_date',
                    'expected_results_en':'expected_results',
                    'prog_name_en':'program_name',
            })\
            .merge(ent_npk_id, how='left', left_on=['recipient_id'], right_on=['external_id'])\
            .fillna({'ent_sks_id':0})\
            .astype({'ent_sks_id': 'int64'})\
            .drop('external_id',axis=1)\
            .drop_duplicates(subset=['act_sks_id'])\
            .pipe(clean_columns)\
            .to_csv('sks_activities_{}.csv'.format(timestamp), header=True, index=False, encoding='utf-8-sig')
        
        print("Data processed")

        acts_es_upload = df[
            'act_sks_id',
            'grant_title',
            'recipient_organization',
            'grant_description',
            'grant_region',
            'grant_municipality',
            'expected_results',
            'type'
        ]
        acts_es_upload.to_csv('./data/processed/sks_activities_es_{}.csv'.format(timestamp),index=False,header=True)
   
        print('ES Upload CSV (activities) exported')
    
    except:
        traceback.print_exc()

if __name__ == "__main__":
  main()