__author__ = 'sgangichetty'
from flask import Flask, Response
from flask_restful import Resource, Api, reqparse
import sqlite3, pandas as pd

app = Flask(__name__)
api = Api(app)


# defining a forecasting resource
class ForecastAPI(Resource):
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('region', type=str)
        parser.add_argument('type', type=str)
        request_data = parser.parse_args()

        # make sure we prep the arguments to be processed properly
        if request_data['region']:
            region_arg = request_data['region'].lower()
        if request_data['type']:
            type_arg = request_data['type'].lower()

        # build the where clause depending on the options
        if request_data['region'] and request_data['type']:
            where_clause = "where lower(region) = {} and lower(type) = {}".format(region_arg,
                                                                                  type_arg)
        elif request_data['region'] is None and request_data['type']:
            where_clause = 'where lower(type) = {}'.format(type_arg)

        elif request_data['type'] is None and request_data['region']:
            where_clause = 'where lower(region) = {} and lower(type) is null'.format(region_arg)

        else:
            where_clause = 'where lower(region) is null and lower(type) is null'

        # connect and fetch results
        connection = sqlite3.connect('gemcast.db')
        df = pd.read_sql("select * from l1_forecast_results {}".format(where_clause), connection)
        resp = Response(response=df.to_json(),
                        status=200,
                        mimetype="application/json")
        return resp


api.add_resource(ForecastAPI, '/forecast', endpoint='forecast')

if __name__ == '__main__':
    app.run(debug=True)
