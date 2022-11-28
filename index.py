from flask import Flask, request, Response
from flask_cors import CORS
from flask import json
import pandas as pd

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'
app.config['CORS_HEADERS'] = 'Access-Control-Allow-Origin'

from controllers.activities_controller import (
    get_test_activity,
    get_activities_per_entity,
    get_5_most_recent_activities_per_entity,
    get_activity_postgres)

from controllers.entities_controller import (
    get_test_entity,
    get_entity_postgres
)

from controllers.elasticsearch_controller import (
    connect_elasticsearch,
    search_all_records,
    search_records,
    count_records,
    format_download
)


def comma_separated_params_to_list(param):
    result = []
    for val in param.split(','):
        if val:
            result.append(val)
    return result


es = connect_elasticsearch()

app = Flask(__name__)

white = [
    'http://localhost:3000',
    'http://0.0.0.0:3000',
    'https://sks-interface-l5jum.ondigitalocean.app',
    'https://sectorknowledge.ca',
    'https://sks-interface-ajah-hxdf7.ondigitalocean.app',
    'https://www.sectorknowledge.ca'
]


@app.after_request
def add_cors_headers(response):

    response.headers.add('Access-Control-Allow-Credentials', 'true')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type')
    response.headers.add('Access-Control-Allow-Headers', 'Cache-Control')
    response.headers.add('Access-Control-Allow-Headers', 'X-Requested-With')
    response.headers.add('Access-Control-Allow-Headers', 'Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, PUT, DELETE')

    if request.referrer:
        r = request.referrer[:-1]
        if r in white:
            response.headers.add('Access-Control-Allow-Origin', r)

    return response


@app.route('/')
def home():
    return "Sector Knowledge Sharing Project"


@app.route('/search', methods=['GET'])
def search(municipality="", region="", terms=""):
    keyword = request.args.get("q")
    # Filter forms a list of the types in the URL to pass to ES
    doctype = [r for r in request.args.get("doctype").split(",")]
    operator = request.args.get("operator")
    municipality = request.args.get("city")
    region = request.args.get("region")
    terms = request.args.get("terms")  # [r for r in request.args.get("terms").split(",")]
    if len(keyword) > 1:
        es.indices.refresh(index="*")
        return search_records(
            keyword=keyword,
            doctype=doctype,
            operator=operator,
            municipality=municipality,
            region=region,
            terms=terms,
            size=100
        )
    else:
        pass


@app.route('/download', methods=['GET'])
def download():
    keyword = request.args.get("q")
    doctype = [r for r in request.args.get("doctype").split(",")]
    operator = request.args.get("operator")
    municipality = request.args.get("city")
    region = request.args.get("region")
    if len(keyword) > 1:
        es.indices.refresh(index="*")
        data = search_records(
            keyword=keyword,
            doctype=doctype,
            operator=operator,
            municipality=municipality,
            region=region,
            size=10000
        )
        # data = search_records(keyword=keyword,doctype=doctype,operator=operator,size=10000)
        csv = format_download(data)
        return Response(
            csv,
            mimetype="text/csv",
            headers={"Content-disposition":
                     "attachment; filename=sks-download.csv"})
    else:
        pass


@app.route('/search-all/<index>', methods=['GET'])
def search_all(index):
    es.indices.refresh(index="*")
    return search_all_records(index=index)


@app.route('/count', methods=['GET'])
def count():
    keyword = request.args.get("q")
    operator = request.args.get("operator")
    municipality = request.args.get("municipality")
    region = request.args.get("region")
    if len(keyword) > 1:
        es.indices.refresh(index="*")
        return count_records(
            keyword=keyword,
            operator=operator,
            municipality=municipality,
            region=region
        )
    else:
        pass


@app.route('/testentity')
def test_entity():
    data = get_test_entity(),
    response = app.response_class(
        response=json.dumps(data),
        status=200,
        mimetype='application/json'
    )

    return response


@app.route('/testactivity')
def test_activity():
    data = get_test_activity(),
    response = app.response_class(
        response=json.dumps(data),
        status=200,
        mimetype='application/json'
    )
    return response


@app.route('/activities/<_id>', methods=['GET'])
def get_one_activity(_id):
    data = get_activity_postgres(_id)
    response = app.response_class(
        response=(data),
        status=200,
        mimetype='application/json'
    )

    return response


@app.route('/activities/byentity/<ent_id>', methods=['GET'])
def get_activities_by_entity(ent_id):
    data = get_activities_per_entity(ent_id)
    response = app.response_class(
        response=(data),
        status=200,
        mimetype='application/json'
    )

    return response


@app.route('/activities/mostrecentbyent/<ent_id>', methods=['GET'])
def get_recent_activities_by_entity(ent_id):
    e_id = ent_id  # '{}.0'.format(ent_id) # Hacky workaround to accomodate dodgy columns before fixing
    print(e_id)
    data = get_5_most_recent_activities_per_entity(e_id)
    response = app.response_class(
        response=(data),
        status=200,
        mimetype='application/json'
    )
    return response


@app.route('/entities/<_id>')
def get_one_entity(_id):
    data = get_entity_postgres(_id=_id)
    response = app.response_class(
        response=(data),
        status=200,
        mimetype='application/json'
    )
    return response


@app.route("/testCSV")
def getPlotCSV():
    csv = '1,2,3\n4,5,6\n'
    return Response(
        csv,
        mimetype="text/csv",
        headers={"Content-disposition":
                 "attachment; filename=test.csv"})


if __name__ == "__main__":
    app.run(debug=True)
