import json
import requests

from flask import abort, make_response, jsonify

import _connect as connection
import queries as query

# -------------------------- CONSTANT ---------------------------------
QUERY_URL = connection.read_cred()['env']['PREDIX_DATA_TIMESERIES_QUERY_URI']


def query_getLatestValue(tag_id):
    response_data = requests.post(QUERY_URL + '/latest',
                                  data=bytes(json.dumps(query.createLatestValues(tag_id))),
                                  headers=connection.create_header())
    print(response_data)
    if response_data.raise_for_status() is None:
        return response_data.json()
    abort(make_response(jsonify({'response': {'message': ' Forwarding data from predix response.'}}),
                        response_data.raise_for_status))


# Query for aggragation requests
def query_agg_data(tag_id, req_agg_func, start_time, end_time):
    response_data = requests.post(QUERY_URL,
                                  data=bytes(json.dumps(query.create_agg_query_body(tag_id, start_time, end_time, req_agg_func))),
                                  headers=connection.create_header())
    print(response_data)
    if response_data.raise_for_status() is None:
        return response_data.json()
    abort(make_response(jsonify({'response': {'message': ' Forwarding data from predix response.'}}),
                        response_data.raise_for_status))


# Query latest values including null values
def query_LatestValue(tag_id):
    response_data = requests.post(QUERY_URL + '/latest',
                                  data=bytes(json.dumps(query.createLatestValues(tag_id))),
                                  headers=connection.create_header())
    print(response_data)
    if response_data.raise_for_status() is None:
        return response_data.json()
    abort(make_response(jsonify({'response': {'message': ' Forwarding data from predix response.'}}),
                        response_data.raise_for_status))


def query_getLimitedValue(tag_id):
    response_data = requests.post(QUERY_URL,
                                  data=bytes(json.dumps(query.createLimitedValues_Query(tag_id))),
                                  headers=connection.create_header())
    print(response_data)
    if response_data.raise_for_status() is None:
        return response_data.json()
    abort(make_response(jsonify({'response': {'message': ' Forwarding data from predix response.'}}),
                        response_data.raise_for_status))


# --------------------------------------------------------------------------------------------------
def query_aggregated_func(tag_id, start_time, end_time, time_sp, val, type_inp):
    response_data = requests.post(QUERY_URL,
                                  data=bytes(json.dumps(query.aggregated_body(tag_id, start_time, end_time, time_sp, val, type_inp))),
                                  headers=connection.create_header())
    if response_data.raise_for_status() is None:
        return response_data.json()
    abort(make_response(jsonify({'response': {'message': ' Forwarding data from predix response.'}}),
                        response_data.raise_for_status))


def query_year_count(tag_id):
    response_data = requests.post(QUERY_URL,
                                  data=bytes(json.dumps(query.year_count(tag_id))),
                                  headers=connection.create_header())
    print(response_data)
    if response_data.raise_for_status() is None:
        return response_data.json()
    abort(make_response(jsonify({'response': {'message': ' Forwarding data from predix response.'}}),
                        response_data.raise_for_status))


def query_aggregated_func_max(tag_id, start_time, end_time, time_sp, val, type_inp):
    response_data = requests.post(QUERY_URL,
                                  data=bytes(json.dumps(query.aggregated_body(tag_id, start_time, end_time, time_sp, val, type_inp))),
                                  headers=connection.create_header())
    print(response_data)
    if response_data.raise_for_status() is None:
        return response_data.json()
    abort(make_response(jsonify({'response': {'message': ' Forwarding data from predix response.'}}),
                        response_data.raise_for_status))


def query_aggregated_func_min(tag_id, start_time, end_time, time_sp, val, type_inp):
    response_data = requests.post(QUERY_URL,
                                  data=bytes(json.dumps(query.aggregated_body(tag_id, start_time, end_time, time_sp, val, type_inp))),
                                  headers=connection.create_header())
    print(response_data)
    if response_data.raise_for_status() is None:
        return response_data.json()
    abort(make_response(jsonify({'response': {'message': ' Forwarding data from predix response.'}}),
                        response_data.raise_for_status))


def query_real_value(tag_id):
    response_data = requests.post(QUERY_URL + '/latest',
                                  data=bytes(json.dumps(query.real_value(tag_id))),
                                  headers=connection.create_header())
    print(response_data)
    if response_data.raise_for_status() is None:
        return response_data.json()
    abort(make_response(jsonify({'response': {'message': ' Forwarding data from predix response.'}}),
                        response_data.raise_for_status))


def query_get_timeseries_tags():
    queryURL = connection.read_cred()['env']['PREDIX_DATA_TIMESERIES_QUERY_TAGS']
    response_data = requests.get(queryURL,
                                 headers=connection.create_header())
    return response_data.json()


def query_get_tags(classifier, classifier_id):
    queryURL = connection.read_cred()['env']['PREDIX_DATA_ASSET_URI'] + "/" + classifier + "/" + classifier_id
    response_data = requests.get(queryURL,
                                 headers=connection.create_asset_header())
    return response_data.json()


def query_delete_tags(classifier, classifier_id, old_value):
    payload = json.dumps(query_get_tags(classifier, classifier_id))
    data = json.loads(payload)
    data[0]['values'].remove(old_value)
    payload = json.dumps(data)
    queryURL = connection.read_cred()['env']['PREDIX_DATA_ASSET_URI'] + "/" + classifier + "/" + classifier_id
    response_data = requests.put(queryURL,
                                 data=payload,
                                 headers=connection.create_asset_header())
    return response_data


def query_put_tags(classifier, classifier_id, new_value):
    data = json.loads(json.dumps(query_get_tags(classifier, classifier_id)))
    data[0]['values'].append(new_value)
    payload = json.dumps(data)
    queryURL = connection.read_cred()['env']['PREDIX_DATA_ASSET_URI'] + "/" + classifier + "/" + classifier_id
    response_data = requests.put(queryURL,
                                 data=payload,
                                 headers=connection.create_asset_header())
    return response_data


def query_zero_value(tag_id):
    response_data = requests.post(QUERY_URL,
                                  data=bytes(json.dumps(query.zero_count(tag_id))),
                                  headers=connection.create_header())
    print(response_data)
    if response_data.raise_for_status() is None:
        return response_data.json()
    abort(make_response(jsonify({'response': {'message': ' Forwarding data from predix response.'}}),
                        response_data.raise_for_status))


def query_time_bound_data(tag_id,endtime):
    ret_data = requests.post(QUERY_URL,
                             data=bytes(json.dumps(query.time_bound_data(tag_id,endtime))),
                             headers=connection.create_header())
    if ret_data.raise_for_status() is None:
        return ret_data.json()
    abort(make_response(jsonify({'response': {'message': ' Forwarding data from predix response.'}}), ret_data.raise_for_status))