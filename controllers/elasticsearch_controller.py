import json
import pandas as pd
import csv
from elasticsearch import Elasticsearch, helpers
from os.path import join, dirname
from dotenv import load_dotenv
import pprint
import sys
import os
from .custom_filters import CUSTOM_FILTERS

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


def connect_elasticsearch():
    # es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    es_username = os.getenv('ES_USERNAME')
    es_password = os.getenv('ES_PASSWORD')
    es = Elasticsearch(
        cloud_id=os.getenv('ES_CLOUD_ID'),
        http_auth=(es_username, es_password),
    )

    if es.ping():
        print('Connected to ES')
    else:
        print('ES could not connect')

    return es


es = connect_elasticsearch()


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


def generate_location_params(municipality, region):
    """
    Generates location params based on whether municipality and region are passed
    Note these are passed as "should" with a varying minimum_should_match param
    1. municipality
        a. and region (#1)
        b. no region (#2)
    2. region 
        a. and no municipality (#3)
    3. neither = pass  (#4)
    """
    mun_params = []
    reg_params = []
    should = dict()

    if municipality:
        mun_params = generate_mun_params(municipality)
        if region:
            reg_params = generate_reg_params(region)
            should = reg_params + mun_params

        else:
            should = mun_params

    elif not municipality:
        if region:
            should = generate_reg_params(region)
        else:
            pass

    return should


def gen_term_filter_blocks(term):
    base = {
        "multi_match": {
            "query": term,
            "type": "phrase",
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
    }

    return base


def generate_term_params(terms):

    include = []
    exclude = []
    term_params = [t for t in terms.split(',')]

    term_dict = CUSTOM_FILTERS

    for t in term_dict.keys():
        if t in term_params:
            for x in term_dict[t]['include']:
                block = gen_term_filter_blocks(x)
                include.append(block)
            for x in term_dict[t]['exclude']:
                block = gen_term_filter_blocks(x)
                exclude.append(block)

    return include, exclude


def build_filter(municipality=None, region=None, terms=None):
    """
    Cases to accommodate:
        1. No terms
            if location
                should = location_params
            if not location
                pass
        2. Some term/s
            if location
                should = location_parmas
                must = term_params
            not location
                must = term_params
    """
    # Set up bool filter
    bool_filter = dict()
    bool_filter['bool'] = {}
    params = []

    if (municipality or region):
        location_params = generate_location_params(municipality, region)
        params.extend(location_params)

    bool_filter['bool']['minimum_should_match'] = int(len(params) / 2)
    bool_filter['bool']['should'] = params

    return [bool_filter]


def build_query(keyword, operator, municipality, region, terms=None, size=None):

    query = {
        "query": {
            "bool": {}
        }
    }

    keyword_block = {
        "multi_match": {
            "query": keyword,
            "fields": [
                "grant_title",
                "grant_description",
                "recipient_organization",
                "expected_results",
                "program_name",
                "name",
                "focus_area",
                "website_text"
            ],
            "operator": operator
        }
    },

    if terms:
        include, exclude = generate_term_params(terms)
        query['query']['bool']['should'] = include
        query['query']['bool']['minimum_should_match'] = 1
        query['query']['bool']['must_not'] = exclude

        # Terms and keyword
        if keyword:
            query['query']['bool']['must'] = keyword_block

        # Terms but no keyword
        else:
            # Don't add anything else
            pass

    else:
        # No terms, but keyword
        if keyword:
            query['query']['bool']['must'] = keyword_block
        # No terms or keyword, return match_all
        else:
            query['query']['bool']['must'] = {
                "match_all": {}
            }

    if municipality or region:
        filter = build_filter(municipality=municipality, region=region)
        query['query']['bool']['filter'] = filter

    if size is not None:
        query['size'] = size

    return query


def count_records(keyword, operator, municipality=None, region=None, terms=None, es=es):
    query = build_query(keyword, operator, municipality, region, terms)

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
    csv = pd.DataFrame([x for x in hits]).to_csv()
    return csv


# Functions for testing

def handle_params(param, params):
    p = [x for x in params if param in x]
    result = ''
    if p:
        result = p[0].replace('{}='.format(param), '')

    return result


def extract_query_params(link):
    _, after = link.split("?")
    params = after.split("&")

    q = handle_params('q', params)
    # doctype = handle_params('doctype', params)
    operator = handle_params('operator', params)
    municipality = handle_params('municipality', params)
    region = handle_params('region', params)
    terms = handle_params('terms', params)

    return q, operator, municipality, region, terms


if __name__ == '__main__':
    # Test cases
    link = "http://127.0.0.1:5000/search?q=&doctype=activity,entity&municipality=&operator=and&region=&terms=efc_sustainability"
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
