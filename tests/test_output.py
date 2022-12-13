from controllers.custom_filters import CUSTOM_FILTERS
from controllers.elasticsearch_controller import extract_query_params
import requests

link = "https://sks-server-ajah-ttwto.ondigitalocean.app/search?q=waste&city=&doctype=activity,entity&operator=and&region=&terms=efc_sustainability"

r = requests.get(link)
data = r.json()
q, operator, municipality, region, terms = extract_query_params(link)


def check_hits(hit, keyword, term):
    terms = CUSTOM_FILTERS[term]['include']
    vals = list(hit['_source'].values())
    str_vals = [v for v in vals if isinstance(v, str)]
    blob = ' '.join(str_vals).lower().replace('-', ' ')

    # Check keyword in blob
    assert keyword in blob, "Keyword not found"

    # Check terms in blob
    assert terms[0] in blob or terms[1] in blob or terms[2] in blob, "Terms not found"


def test_output():
    for h in data['hits']:
        check_hits(h, q, terms)
