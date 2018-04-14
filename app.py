import os
import sys
from monthdelta import monthdelta
from flask import Flask, request, abort, jsonify, make_response
from flask_cors import CORS
from waitress import serve

from ResponseDict import *

sys.path.insert(0, 'Reon')
sys.path.append("Assets")
sys.path.append("Queries")
import Reon as reonService
import Assets as assets
import _connect as con
app = Flask(__name__)
CORS(app)
port = int(os.getenv("PORT", 8000))


@app.route('/api/token')
def get_verify_auth_token():
    if reonService.verify_auth_token(request.args.get('tok')):
        return make_response(jsonify({'access': 'Granted!'}), 200)
    abort(make_response(jsonify(Auth_Token_Expired), 401))


@app.route("/api/login")
def get_login():
    validate = reonService.get_login(request.args.get('u'), request.args.get('p'))
    if validate[0]:
        return make_response(jsonify(validate[1]), 200)
    abort(make_response(jsonify(validate[1]), 401))


@app.route("/api/aggregated_day")
def get_aggregated_day():
    if reonService.verify_auth_token(request.args.get('tok')):
        return reonService.get_aggregated_day(request.args.get('t'), request.args.get('time'))
    abort(make_response(jsonify(Auth_Token_Expired), 401))


@app.route("/api/aggregated_week")
def get_aggregated_week():
    if reonService.verify_auth_token(request.args.get('tok')):
        return reonService.get_aggregated_week(request.args.get('t'), request.args.get('time'))
    abort(make_response(jsonify(Auth_Token_Expired), 401))


@app.route("/api/return_weeks")
def day_returns():
    if reonService.verify_auth_token(request.args.get('tok')):
        return reonService.get_return_days(request.args.get('t'), request.args.get('time'))
    abort(make_response(jsonify(Auth_Token_Expired), 401))


@app.route("/api/return_months")
def week_returns():
    if reonService.verify_auth_token(request.args.get('tok')):
        return reonService.get_return_weeks(request.args.get('t'), request.args.get('time'))
    abort(make_response(jsonify(Auth_Token_Expired), 401))


@app.route("/api/aggregated_month")
def get_aggregated_month():
    if reonService.verify_auth_token(request.args.get('tok')):
        return reonService.get_aggregated_month(request.args.get('t'), request.args.get('time'))
    abort(make_response(jsonify(Auth_Token_Expired), 401))


@app.route("/api/aggregated_year")
def get_aggregated_year():
    if reonService.verify_auth_token(request.args.get('tok')):
        return reonService.get_aggregated_year(request.args.get('t'),request.args.get('time'))
    abort(make_response(jsonify(Auth_Token_Expired), 401))

@app.route("/api/infinite")
def get_infinite():
    if reonService.verify_auth_token(request.args.get('tok')):
        return reonService.get_infinite(request.args.get('t'),request.args.get('time'))
    abort(make_response(jsonify(Auth_Token_Expired), 401))


@app.route("/api/aggregated_min")
def get_aggregated_min():
    if reonService.verify_auth_token(request.args.get('tok')):
        return reonService.get_aggregated_min(request.args.get('t'), request.args.get('time'))
    abort(make_response(jsonify(Auth_Token_Expired), 401))


@app.route("/api/aggregated_max")
def get_aggregated_max():
    if reonService.verify_auth_token(request.args.get('tok')):
        return reonService.get_aggregated_max(request.args.get('t'), request.args.get('time'))
    abort(make_response(jsonify(Auth_Token_Expired), 401))


@app.route("/api/real_val")
def get_real_value():
    if reonService.verify_auth_token(request.args.get('tok')):
        return reonService.get_real_value(request.args.get('t'))
    abort(make_response(jsonify(Auth_Token_Expired), 401))


@app.route("/api/zero_count")
def get_zero_count():
    if reonService.verify_auth_token(request.args.get('tok')):
        return reonService.get_zero_count(request.args.get('t'))
    abort(make_response(jsonify(Auth_Token_Expired), 401))


@app.route("/getalltags")
def getAllTags():
    if reonService.verify_auth_token(request.args.get('tok')):
        classifier = request.args.get('t')
        classifier_id = request.args.get('id')
        return assets.get_tags(classifier, classifier_id)
    abort(make_response(jsonify(Auth_Token_Expired), 401))


@app.route("/gettimeseriestags")
def getTimeSeriesTags():
    if reonService.verify_auth_token(request.args.get('tok')):
        return assets.get_timeseries_tags()
    abort(make_response(jsonify(Auth_Token_Expired), 401))


@app.route("/putalltags")
def putAllTags():
    if reonService.verify_auth_token(request.args.get('tok')):
        classifier = request.args.get('t')
        classifier_id = request.args.get('id')
        new_value = request.args.get('v')
        return make_response(jsonify({'message': 'Tag Inserted'}), 201)
    abort(make_response(jsonify(Auth_Token_Expired), 401))


@app.route("/deletetags")
def deleteTags():
    if reonService.verify_auth_token(request.args.get('tok')):
        classifier = request.args.get('t')
        classifier_id = request.args.get('id')
        old_value = request.args.get('v')
        return make_response(jsonify({'message': 'Tag Deleted'}), 201)
    abort(make_response(jsonify(Auth_Token_Expired), 401))


if __name__ == "__main__":
    # app.run(host='0.0.0.0', port=port)
    serve(app, host='0.0.0.0', port=port)