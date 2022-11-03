import json
import pandas as pd
import csv
from elasticsearch import Elasticsearch, helpers
from os.path import join, dirname
from dotenv import load_dotenv
import pprint
import json
import sys
import os

# decrease the maxInt value by factor 10 as long as the OverflowError occurs.
maxInt = sys.maxsize
while True:
    try:
        csv.field_size_limit(maxInt)
        break
    except OverflowError:
        maxInt = int(mgit axInt / 10)

pp = pprint.PrettyPrinter(indent=2)

dotenv_path = join(dirname(__file__), '.env')
load_dotenv()

ents_es = "sks-backend/data/processed/ents_es_upload.csv"


def csv_to_json(filepath):
    df = pd.read_csv(filepath)
    json_df = df.to_json(orient='records')
    return json.loads(json_df)


def connect_elasticsearch():
    # es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    es_username = os.getenv('ES_USERNAME')
    es_password = os.getenv('ES_PASSWORD')
    es = Elasticsearch(
        cloud_id="SKS_Project:bm9ydGhhbWVyaWNhLW5vcnRoZWFzdDEuZ2NwLmVsYXN0aWMtY2xvdWQuY29tJGNlZGUyYjk4M2ZmZjRlYWI5ZDZkNzMxZmM3Nzc1YzU4JGU2ZDA1OTFmYmViZjRjY2Y4YWYxZDJlNGE5MzhiZmEx",
        http_auth=(es_username, es_password),
    )

    if es.ping():
        print('Connected to ES')
    else:
        print('ES could not connect')

    return es


es = connect_elasticsearch()

# Bulk upload working for entities


def bulk_upload(doc, index, es=es):
    with open(doc, 'r') as outfile:
        reader = csv.DictReader(outfile)
        next(reader)  # Skips header row
        helpers.bulk(es, reader, index=index, doc_type="type")

# Bulk upload not working for activities, adding line by line until resolved


def upload_data(doc, bool, index, es=es):
    if bool == True:
        data = csv_to_json(doc)
        for d in data:
            r = store_record(es, index, d)


def store_record(es_object, index, data):
    is_stored = True

    try:
        outcome = es_object.index(
            index=index,
            doc_type='_doc',
            body=json.dumps(data))
        print(outcome['result'])
    except Exception as ex:
        print('Error in indexing data')
        print(str(ex))
        is_stored = False
    finally:
        return is_stored


def delete_index(index, es=es):
    try:
        es.indices.delete(index=index, ignore=[400, 404])
    except Exception as ex:
        print(ex)


def search_all_records(index, es=es):
    query = {"query": {"match_all": {}}}
    res = es.search(index=index, body=query)
    pp.pprint("Total hits: {}".format(res['hits']['total']))

    return res['hits']

# TODO: Refactor count_records & search_records so query is not repeated


def generate_reg_params(region):
    grant_reg = {}
    grant_reg['match'] = {"grant_region": region}

    location_reg = {}
    location_reg['match'] = {"location_region": region}

    should_reg = []
    should_reg.extend([grant_reg, location_reg])
    return should_reg


def generate_mun_params(municipality):
    grant_mun = {}
    grant_mun['match'] = {"grant_municipality": municipality}

    location_mun = {}
    location_mun['match'] = {"location_municipality": municipality}

    should_mun = []
    should_mun.extend([grant_mun, location_mun])
    return should_mun


def build_filter(municipality=None, region=None):
    mun_params = []
    reg_params = []

    if municipality is not None:
        mun_params = generate_mun_params(municipality)
    if region is not None:
        reg_params = generate_reg_params(region)

    should = []
    should = reg_params + mun_params
    filter_list = [
        {"bool": {"should": should}}
    ]
    return filter_list


def build_query(keyword, operator, municipality, region, size=None):
    query = {
        "query": {
            "bool": {
                "must": {
                    "simple_query_string": {
                        "query": keyword,
                        "fields": [
                            "grant_title",
                            "grant_region",
                            "grant_region",
                            "recipient_organization",
                            "grant_municipality",
                            "expected_results",
                            "program_name",
                            "name",
                            "focus_area",
                            "location_municipality",
                            "location_region",
                            "location_country"
                        ],
                        "default_operator": operator
                    }
                },
                "filter": build_filter(municipality=municipality, region=region)
            }
        }
    }

    if size is not None:
        query['size'] = size

    return query


def count_records(keyword, operator, municipality=None, region=None, es=es):
    query = build_query(keyword, operator, municipality, region)

    resp_dict = {}

    for i in ['new-activities,entities', 'new-activities', 'entities']:
        resp = es.count(index=i, body=query)
        result = resp['count']
        resp_dict[i] = result

    return resp_dict


def search_records(keyword, doctype, operator, size=None,
                   municipality=None, region=None, index=None, es=es):

    query = build_query(keyword, operator, municipality, region, size)
    index = None
    if 'activity' in doctype and 'entity' in doctype:
        index = "new-activities,entities"
        # index = "*"
    elif doctype == ['activity']:
        index = 'new-activities'
    elif doctype == ['entity']:
        index = 'entities'

    res = es.search(index=index, body=query)

    return res['hits']


def format_download(data):
    hits = [d['_source'] for d in data['hits']]
    csv = pd.DataFrame([x for x in hits]).drop(columns='Unnamed: 0').to_csv()
    return csv


# if __name__ == '__main__':
    # upload_data(acts_es,True, index='new-activities')
    # upload_data(ents_es,True, index='entities')
