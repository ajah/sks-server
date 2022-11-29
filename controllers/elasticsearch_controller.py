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
        maxInt = int(maxInt / 10)

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
        outcome = es_object.index(index=index, doc_type='_doc', body=json.dumps(data))
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
    grant_reg['match_phrase'] = {"grant_region": region}

    location_reg = {}
    location_reg['match_phrase'] = {"location_region": region}

    should_reg = []
    should_reg.extend([grant_reg, location_reg])
    return should_reg


def generate_mun_params(municipality):
    grant_mun = {}
    grant_mun['match_phrase'] = {"grant_municipality": municipality}

    location_mun = {}
    location_mun['match_phrase'] = {"location_municipality": municipality}

    should_mun = []
    should_mun.extend([grant_mun, location_mun])

    return should_mun


def handle_terms(terms):
    result = None
    if terms:
        if 'efc_sustainability' in terms:
            result = 'sustainability'

    return result


def generate_term_params(terms):
    """
    "grant_title",
    "Recipient_organization",
    "Expected_results",
    "Program_name",
    "Name",
    "focus_area",
    """
    term_list = handle_terms(terms)
    must_terms = dict()
    must_terms['multi_match'] = {
        "query": term_list,
        "fields": [
            "grant_title",
            "grant_description",
            "expected_results",
            "program_name",
            "name",
            "focus_area",
            "website_text"
        ]
    }

    return must_terms


def build_filter(municipality=None, region=None, terms=None):
    """
    Cases to accommodate: 
        1. No filters
        2. At least one filter
    """
    # Extend so that it contains the terms in the "must" column
    mun_params = []
    reg_params = []
    term_params = []

    # If either terms or municipality
    if municipality:
        mun_params = generate_mun_params(municipality)

    if region:
        reg_params = generate_reg_params(region)

    # SEt up bool obj

    bool_filter = dict()

    # Case if terms
    if terms:
        bool_filter['bool'] = {}
        term_params = generate_term_params(terms)
        bool_filter['bool']['must'] = term_params

    # Case if mun and reg
    if municipality and region:
        should = reg_params + mun_params
        bool_filter['bool'] = {}
        bool['should'] = should
        bool_filter['bool']['minimum_should_match'] = 2

    # Case if mun and not reg
    if municipality and not region:
        bool_filter['bool'] = {}
        bool_filter['bool']['should'] = mun_params
        bool_filter['bool']['minimum_should_match'] = 1

    # Case if reg and not mun
    elif region and not municipality:
        bool_filter['bool'] = {}
        bool_filter['bool']['should'] = reg_params
        bool_filter['bool']['minimum_should_match'] = 1

    # # If all are none
    # elif not municipality and not region and not terms:
    #     bool_filter = None

    return [bool_filter]


def build_query(keyword, operator, municipality, region, terms=None, size=None):
    filter = []
    if municipality or region or terms:
        filter = build_filter(municipality=municipality, region=region, terms=terms)
    query = {
        "query": {
            "bool": {
                "must": {
                    "multi_match": {
                        "query": keyword,
                        "fields": [
                            "grant_title",
                            "grant_description",
                            # "grant_region",
                            "recipient_organization",
                            # "grant_municipality",
                            "expected_results",
                            "program_name",
                            "name",
                            "focus_area",
                            "website_text"
                            # "location_municipality",
                            # "location_region",
                            # "location_country"
                        ],
                        "operator": operator
                    }
                },
                "filter": filter
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
                   municipality=None, region=None, terms=None, index=None, es=es):
    # terms = handle_terms(terms)
    query = build_query(keyword, operator, municipality, region, terms, size)
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


# Functions for testing

def handle_params(param, params):
    p = [x for x in params if param in x]
    result = ''
    if p:
        result = p[0].replace('{}='.format(param), '')

    return result


def extract_query_params(link):
    before, after = link.split("?")
    params = after.split("&")

    q = handle_params('q', params)
    # doctype = handle_params('doctype', params)
    operator = handle_params('operator', params)
    municipality = handle_params('municipality', params)
    region = handle_params('region', params)
    terms = handle_params('terms', params)

    return q, operator, municipality, region, terms


if __name__ == '__main__':
    # search?q=environment%20water&city=Toronto&doctype=activity,organization&operator=or&region=on&terms=efc_sustainability
    # test_query = build_query(keyword='environment water', municipality='Toronto',
    #                          operator='or', region='on',
    #                          #  terms=''
    #                          terms='efc_sustainability'
    #                          )
    # link = "http://127.0.0.1:5000/search?q=environment&doctype=activity,entity&municipality=toronto&operator=and&region=&terms=efc_sustainability"
    link = "http://127.0.0.1:5000/search?q=environment&doctype=activity,entity&municipality=&operator=and&region=&terms="
    q, operator, municipality, region, terms = extract_query_params(link)
    test_query = build_query(
        keyword=q,
        operator=operator,
        municipality=municipality,
        region=region,
        terms=terms
    )
    print(json.dumps(test_query))
    # upload_data(acts_es,True, index='new-activities')
    # upload_data(ents_es,True, index='entities')
