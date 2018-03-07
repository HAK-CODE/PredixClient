import calendar
import datetime
import json
import time
from cachetools import TTLCache
from cachetools import cached
import dateutil.parser
import pandas as pd
import querybody as qb
from dateutil import tz
from itsdangerous import (TimedJSONWebSignatureSerializer
                          as Serializer, BadSignature, SignatureExpired)
from joblib import Parallel, delayed

import util

# ----------------------------------------------
username = 'reonservicesSpark'
passwd = 'reonservices@pRe_dix'
cache = TTLCache(maxsize=10, ttl=120)
# ---------------------------------------------------------------------------


# 24 time call
def get_aggregated_day(tag_id, get_time):
    new_time = []
    df = datetime.datetime.strptime(get_time + ' 00:00:00', "%Y-%m-%d %H:%M:%S")
    for i in range(25):
        df += datetime.timedelta(hours=1)
        new_time.append(int(time.mktime(utc_return_nowtime(df).timetuple())) * 1000)
    return util.parse_data_reon(Parallel(n_jobs=15, backend="threading")
                                (delayed(qb.query_aggregated_func)
                                 (tag_id, new_time[j], new_time[j + 1], 's', 15, 'avg') for j in range(24)))


def get_return_days(tag_id, get_time):
    res = json.loads(get_aggregated_week(tag_id, get_time))
    resList = []
    for i in range(len(res['response']['results'])):
        k = 0
        for j in range(i+1):
            k = k + (res['response']['results'][j][1])
        resList.append([res['response']['results'][i][0], k*20])
    return json.dumps(resList)


@cached(cache)
def get_return_weeks(tag_id, get_time):
    res = json.loads(get_aggregated_month(tag_id, get_time))
    resList = []
    for i in range(len(res['response']['results'])):
        k = 0
        for j in range(i+1):
            k = k + (res['response']['results'][j][1] * 20)
        resList.append([res['response']['results'][i][0], k])
    return json.dumps(resList)


# 7 time call
@cached(cache)
def get_aggregated_week(tag_id, get_time):
    new_time = []
    df = datetime.datetime.strptime(get_time + ' 00:00:00', "%Y-%m-%d %H:%M:%S")
    df = df - datetime.timedelta(days=df.weekday())
    dframe = load_data(tag_id)
    for i in range(7):
        datevalues = dframe.Values[dframe.Date == df.date()].tolist()
        datevalues = [x for x in datevalues if x != 0]
        datevalues = datevalues[::-1]
        if len(datevalues) != 0:
            mid = datevalues[-1] - datevalues[0]    # last - first
            new_time.append([int(time.mktime(utc_return_nowtime(df).timetuple())) * 1000, mid])
        else:
            new_time.append([int(time.mktime(utc_return_nowtime(df).timetuple())) * 1000, 0])

        df += datetime.timedelta(days=1)
    return util.parse_data_reon(new_time, tag_id)


# 4 time call
@cached(cache)
def get_aggregated_month(tag_id, get_time):
    new_time = []
    dframe = load_data(tag_id)

    df1 = datetime.datetime.strptime(get_time + ' 00:00:00', "%Y-%m-%d %H:%M:%S")
    monthDiff = calendar.monthrange(df1.year, df1.month)
    df1 = time_tango(str(df1.year) + "-" + str(df1.month) + "-" + "01")
    for k in range(monthDiff[0], 7):
        df1 += datetime.timedelta(days=1)

    for i in range(4):
        weekList = []
        for j in range(7):
            weekList.append(df1.date())
            df1 += datetime.timedelta(days=1)
        weekSet = set(weekList)
        a = dframe.Date.apply(lambda x: 1 if x in weekSet else 0)
        datevalues = dframe.Values[a == 1].tolist()
        datevalues = [monthDiff for monthDiff in datevalues if monthDiff != 0]
        datevalues = datevalues[::-1]
        if len(datevalues) != 0:
            mid = datevalues[-1] - datevalues[0]    # last - first
            new_time.append([int(time.mktime(utc_return_nowtime(df1 - datetime.timedelta(weeks=1)).timetuple())) * 1000, mid])
        else:
            new_time.append([int(time.mktime(utc_return_nowtime(df1 - datetime.timedelta(weeks=1)).timetuple())) * 1000, 0])

    return util.parse_data_reon(new_time, tag_id)


# 12 time call
def get_aggregated_year(tag_id,get_time):
    alldata = load_data('1_INV_1_TOTAL_ENERGY')
    alldata.Date = pd.to_datetime(alldata['Date'])
    years = list(set(alldata.Date.dt.year))

    pars = dateutil.parser.parse(get_time).date()   
    df1=time_tango(pars)
    print(df1.year)

    if df1.year in years:
        val= json.dumps(qb.query_year_count(tag_id))
        data =  json.loads(val)
        data = data["tags"][0]["results"][0]["values"]
        print(data)
    else:
        data = []
        df1 = time_tango(str(df1.year)+"-01-01")
        for i in range(12):
            data.append([int(time.mktime(utc_return_nowtime(df1 + monthdelta(i+1)).timetuple())) * 1000, 0])
    return util.parse_data_reon(data, tag_id)

def get_aggregated_min(tag_id, get_time):
    new_time = []
    df = datetime.datetime.strptime(get_time + ' 00:00:00', "%Y-%m-%d %H:%M:%S")
    for i in range(25):
        df += datetime.timedelta(hours=1)
        new_time.append(int(time.mktime(utc_return_nowtime(df).timetuple())) * 1000)
    return json.dumps(util.parse_data_reon(Parallel(n_jobs=10)(
        delayed(qb.query_aggregated_func)(tag_id, new_time[j], new_time[j + 1], 's', 15, 'min') for j in range(24))))


def get_aggregated_max(tag_id, get_time):
    new_time = []
    df = datetime.datetime.strptime(get_time + ' 00:00:00', "%Y-%m-%d %H:%M:%S")
    for i in range(25):
        df += datetime.timedelta(hours=1)
        new_time.append(int(time.mktime(utc_return_nowtime(df).timetuple())) * 1000)

    return json.dumps(util.parse_data_reon(Parallel(n_jobs=10)(
        delayed(qb.query_aggregated_func)(tag_id, new_time[j], new_time[j + 1], 's', 15, 'max') for j in range(24))))


def get_real_value(tag_id):
    return json.dumps(qb.query_real_value(tag_id))


def get_zero_count(tag_id):
    return json.dumps(util.parse_data_zeroslen(qb.query_zero_value(tag_id)))


def utc_return(df):
    return datetime.datetime.strptime(str(df), "%Y-%m-%d %H:%M:%S.%f"). \
        replace(tzinfo=tz.tzutc()). \
        astimezone(tz.tzlocal())


def utc_return_nowtime(df):
    return datetime.datetime.strptime(str(df), "%Y-%m-%d %H:%M:%S"). \
        replace(tzinfo=tz.tzutc()). \
        astimezone(tz.tzlocal())


def generate_auth_token(self, expiration=3600):
    s = Serializer('ag7yQAFlQnPCO77B9FIRmaRa2hjZ5oDEVPFjeExiz83Ejy+FatiHEDJW5u5wp9myR+HLbC/QUUUgdcn3CjdBM5M2ee9cvzqlllHHdy2UFolrFFCobnMg5Z67A6XuysRpFWlYsCS2v5pbrOijnK/LjMqGwQ9EfkQ+hTeu27Uu/zUjo5IhsJVpjHyLzYaaQfEP7jfsAVaXQG1Ns9urUorPq+rQnMEdq1W3ad7s+B1GrcOTV8Bk523nB87IkPuxHCDwimAgrEuxdJY=',
                   expires_in=expiration)
    tok = s.dumps('qlUNMq6xzGDNAP3bV3z8crocTkzpUaC83+wLxypbh1Wu2IibGrpf5LFDTTfkpUmwdCA=')
    return tok


def verify_auth_token(token):
    s = Serializer('ag7yQAFlQnPCO77B9FIRmaRa2hjZ5oDEVPFjeExiz83Ejy+FatiHEDJW5u5wp9myR+HLbC/QUUUgdcn3CjdBM5M2ee9cvzqlllHHdy2UFolrFFCobnMg5Z67A6XuysRpFWlYsCS2v5pbrOijnK/LjMqGwQ9EfkQ+hTeu27Uu/zUjo5IhsJVpjHyLzYaaQfEP7jfsAVaXQG1Ns9urUorPq+rQnMEdq1W3ad7s+B1GrcOTV8Bk523nB87IkPuxHCDwimAgrEuxdJY=')
    try:
        data = s.loads(token)
    except SignatureExpired:
        return None  # valid token, but expired
    except BadSignature:
        return None  # invalid token
    # print('returning true')
    return True


def get_login(username, password):
    if username == username and password == passwd:
        return True, {'token': generate_auth_token(3600)}
    else:
        return False, {'access': 'Denied!',
                       'message': 'Wrong Username or Password.'}


def time_tango(date):
    times = "00:00:00"
    return datetime.datetime.strptime("{}, {}".format(date, times), "%Y-%m-%d, %H:%M:%S")


def time_tango_year(date):
    times = "00:00:00"
    return datetime.datetime.strptime("{}, {}".format(date, times), "%Y-%m, %H:%M:%S")


def get_query_time_bound_data(tag_id):
    new_endtime = (int(time.mktime(utc_return(datetime.datetime.now()).timetuple())) * 1000)
    return json.dumps(qb.query_time_bound_data(tag_id, new_endtime))


#@cached(cache)
def load_data(tag_id):
    alldata = json.loads(get_query_time_bound_data(tag_id))
    datelist = []
    valuelist = []
    for i in range(len(alldata['tags'][0]['results'][0]['values'])):
        datelist.append(str(datetime.datetime.fromtimestamp(alldata['tags'][0]['results'][0]['values'][i][0]/1000).strftime('%Y-%m-%d')))
        valuelist.append(alldata['tags'][0]['results'][0]['values'][i][1])

    dframe = pd.DataFrame(columns=['Date', 'Values'])
    dframe['Date'] = datelist
    dframe['Date'] = pd.to_datetime(dframe['Date']).dt.date
    dframe['Values'] = valuelist
    return dframe