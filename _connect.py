import base64
import json
import logging
import os
from datetime import datetime, timedelta
from monthdelta import monthdelta
import dateutil.parser
import requests
import yaml
from flask import Flask
from websocket import create_connection
import time
import app
from threading import Lock

# Initialize the app
app = Flask(__name__, instance_relative_config=True)
lock = Lock()

def client_login():
    client = read_cred()['env']['PREDIX_APP_CLIENT_ID']
    secret = read_cred()['env']['PREDIX_APP_CLIENT_SECRET']
    uaaURL = read_cred()['env']['PREDIX_SECURITY_UAA_URI'] + "/oauth/token"
    credentials = base64.b64encode(str.join(':', [client, secret]))
    headers = {
        'authorization': "Basic " + credentials,
        'cache-control': "no-cache",
        'content-type': "application/x-www-form-urlencoded"
    }
    response = requests.request('POST', uaaURL, data="grant_type=client_credentials", headers=headers)
    if response.status_code == 200:
        logging.debug("RESPONSE=" + str(response.json()))
        return json.loads(response.text)['access_token']
    else:
        logging.warn("Failed to authenticate")
        response.raise_for_status()


def get_credentials(path):
    with open(path, 'r') as stream:
        try:
            return yaml.load(stream)
        except yaml.YAMLError as exception:
            return exception


def get_token(path):
    try:
        with lock:
            if not os.path.isfile(path):
                cache = {
                    'expires': (datetime.now() + timedelta(hours=10)).isoformat(),
                    'token': client_login()
                }
                with open(path, 'w') as outfile:
                    json.dump(cache, outfile)
                return cache['token']
            else:
                read_token = open(path)
                cache = json.load(read_token)
                read_token.close()
                expires = dateutil.parser.parse(cache['expires'])
                if expires < datetime.now():
                    cache = {
                        'expires': (datetime.now() + timedelta(hours=10)).isoformat(),
                        'token': client_login()
                    }
                    with open(path, 'w') as outfile:
                        json.dump(cache, outfile)
                        time.sleep(2)
                        print(cache['token'])
                    return cache['token']
                else:
                    return cache['token']
    except:
        print("Problem opening/reading token file ")


def read_cred():
    return get_credentials(os.path.join(app.root_path, 'manifest_client.yml'))


def create_header():
    HEADER = {
        'Predix-Zone-Id': read_cred()['env']['PREDIX_DATA_TIMESERIES_QUERY_ZONE_ID'],
        'Authorization': 'Bearer ' + get_token(os.path.join(app.root_path, 'token.json')),
        'Content-Type': 'application/json'
    }
    return HEADER


def create_asset_header():
    HEADER = {
        'predix-zone-id': read_cred()['env']['PREDIX_DATA_ASSET_ZONE_ID'],
        'authorization': 'Bearer ' + get_token(os.path.join(app.root_path, 'token.json')),
        'Content-Type': 'application/json'
    }
    return HEADER


def create_analytics_connection(cat_id):
    return read_cred()['env']['PREDIX_ANALYTICS_UAA'] + cat_id + "/execution"


def create_analytics_header():
    HEADER = {
        'predix-zone-id': read_cred()['env']['PREDIX_ANALYTICS_ZONE_ID'],
        'authorization': "Bearer " + get_token(os.path.join(app.root_path, 'token.json')),
        'content-type': "application/json"
    }
    return HEADER


def create_ingest_connection():
    return create_connection(read_cred()['env']['PREDIX_DATA_TIMESERIES_INGEST_URI'], header=create_header())
