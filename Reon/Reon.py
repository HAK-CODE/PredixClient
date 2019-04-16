import calendar
import datetime
from monthdelta import monthdelta
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
from datetime import date, timedelta
import itertools
import pytz
import ast
# ----------------------------------------------
username = 'reonservicesSpark'
passwd = 'reonservices@pRe_dix'
cache = TTLCache(maxsize=10, ttl=120)
# ---------------------------------------------------------------------------


# 24 time call
# def get_aggregated_day(tag_id, get_time):
# 	new_time = []
# 	df = datetime.datetime.strptime(get_time + ' 00:00:00', "%Y-%m-%d %H:%M:%S")
# 	for i in range(25):
# 		new_time.append(int(time.mktime(utc_return_nowtime(df).timetuple()) * 1000))
# 		df += datetime.timedelta(hours=1)
# 	dt=(util.parse_data_reon(Parallel(n_jobs=15, backend="threading")
# 								(delayed(qb.query_aggregated_func)
# 								 (tag_id, new_time[j], new_time[j + 1], 's', 15, 'avg') for j in range(24))))
# 	dt=ast.literal_eval(dt)
# 	dft=[]
# 	dt1=[]
# 	dt_final=[]
# 	dft = dt['response']['results']
# 	dft=[x for x in dft if x!=[]]
# 	for i in range(0,len(dft)):
# 		dt2=(dft[i][0])
# 	# print(dt2)
# 		dt1.append((datetime.datetime.fromtimestamp(dt2/1000)).strftime('%M'))
# 		if(dt1[i]=='00'):
# 			dt_final.append([dft[i][0],dft[i][1]])

# 	return json.dumps(util.parse_data(dt_final,tag_id))

def get_aggregated_day(tag_id, get_time):
	df = datetime.datetime.strptime(get_time + ' 00:00:00', "%Y-%m-%d %H:%M:%S")
	time_list=(int(time.mktime(utc_return_nowtime(df).timetuple()) * 1000))
	end_time=time_list+86340000
	return json.dumps(util.parse_data_latest(qb.query_real_dayvalue(tag_id,time_list,end_time)))

def get_aggregated_day_all(tag_id, get_time):
	df = datetime.datetime.strptime(get_time + ' 00:00:00', "%Y-%m-%d %H:%M:%S")
	time_list=(int(time.mktime(utc_return_nowtime(df).timetuple()) * 1000))
	end_time=time_list+86340000
	return json.dumps(qb.query_real_dayvalue_all(tag_id,time_list,end_time))




# def get_return_days(tag_id, get_time):
# 	res = json.loads(get_aggregated_week(tag_id, get_time))
# 	resList = []
# 	for i in range(len(res['response']['results'])):
# 		k = 0
# 		for j in range(i+1):
# 			k = k + (res['response']['results'][j][1])
# 		resList.append([res['response']['results'][i][0], k*20])
# 	return json.dumps(resList)


# #@cached(cache)
# def get_return_weeks(tag_id, get_time):
# 	res = json.loads(get_aggregated_month(tag_id, get_time))
# 	resList = []
# 	for i in range(len(res['response']['results'])):
# 		k = 0
# 		for j in range(i+1):
# 			k = k + (res['response']['results'][j][1] * 20)
# 		resList.append([res['response']['results'][i][0], k])
# 	return json.dumps(resList)


# 7 time call
#@cached(cache)
def get_aggregated_week(tag_id, get_time):
	new_time = []
	new_list=[]
	dframe = load_data(tag_id)
	pars = dateutil.parser.parse(get_time).date()	
	df=time_tango(pars)
	week_day=df.weekday()
	
	for k in range(week_day):
		df-= datetime.timedelta(days=1)
		
	for i in range(8):
		try:
			if(df.date() <= datetime.datetime.now().date()):
				datevalues = dframe.Values[dframe.Date == df.date()].tolist()
				datevalues = [x for x in datevalues if x != 0]
				datevalues = datevalues[::-1]
				#values1=datevalues[0]
				new_list.append(datevalues[0])
			else:
				new_list.append(0)
		except:
				new_list.append(0)
		df += datetime.timedelta(days=1)
	df = datetime.datetime.strptime(get_time + ' 19:00:00', "%Y-%m-%d %H:%M:%S")
	df = df - datetime.timedelta(days=df.weekday())
	for i in range(len(new_list)-1):
		mid = new_list[i+1] - new_list[i]
		try:
			if(df.date()== datetime.datetime.now().date()):
				mid=datevalues[-1] - datevalues[0]
		except:
			mid=0
		if(mid<0 or new_list[i]==0):
			new_time.append(0)
		else:
			new_time.append(mid)
		df += datetime.timedelta(days=1) 
	return json.dumps(util.parse_data(new_time,tag_id))

def new_week(tag_id, get_time):
	new_time = []
	new_list=[]
	dframe = load_data(tag_id)
	df = datetime.datetime.strptime(get_time + ' 19:00:00', "%Y-%m-%d %H:%M:%S")
	z=first_monday(df.year,df.month)
	y = datetime.datetime(df.year, df.month, 01)
	#df = df - datetime.timedelta(days=df.weekday())
	monthDiff = calendar.monthrange(df.year, df.month)
	start_list = []
	if(df.day>=z.day):
		df = df - datetime.timedelta(days=df.weekday())
		for i in range(8):
			if(df.day<=monthDiff[1]):
				try:
					if(df.date() <= datetime.datetime.now().date()):
						datevalues = dframe.Values[dframe.Date == df.date()].tolist()
						datevalues = [x for x in datevalues if x != 0]
						datevalues = datevalues[::-1]
						#values1=datevalues[0]
						new_list.append(datevalues[0])
					else:
						new_list.append(0)
				except:
					new_list.append(0)

				df += datetime.timedelta(days=1)
		df = datetime.datetime.strptime(get_time + ' 19:00:00', "%Y-%m-%d %H:%M:%S")
		df = df - datetime.timedelta(days=df.weekday())
		save_date = df
		for i in range(len(new_list)-1):
			if(df.day<=monthDiff[1] and df.day >= save_date.day):
				mid = new_list[i+1] - new_list[i]
				try:
					if(df.date()== datetime.datetime.now().date()):
						print(datevalues[-1])
						mid=datevalues[-1] - datevalues[0]
				except:
					mid=0
				if(mid < 0 or new_list[i]==0):
					new_time.append([int(time.mktime(utc_return_nowtime(df).timetuple())) * 1000, 0])
				else:
					new_time.append([int(time.mktime(utc_return_nowtime(df).timetuple())) * 1000, mid])
			df += datetime.timedelta(days=1) 
	else:
		for k in range(y.day,z.day+1):
			if(y.day<=monthDiff[1]):
				try:
					if(y.date() <= datetime.datetime.now().date()):
						datevalues = dframe.Values[dframe.Date == y.date()].tolist()
						datevalues = [x for x in datevalues if x != 0]
						datevalues = datevalues[::-1]
						#values1=datevalues[0]
						new_list.append(datevalues[0])
					else:
						new_list.append(0)
				except:
					new_list.append(0)
				y += datetime.timedelta(days=1)
		y = datetime.datetime(df.year, df.month, 01)
		for i in range(len(new_list)-1):
			if(y.day<=monthDiff[1]):
				mid = new_list[i+1] - new_list[i] 
				try:
					if(y.date()== datetime.datetime.now().date()):
						mid=datevalues[-1]-datevalues[0]
					 # last - first
				except:
					mid=0
				if(mid<0 or new_list[i]==0):
					new_time.append([int(time.mktime(utc_return_nowtime(y).timetuple()) * 1000), 0])
				else:
					new_time.append([int(time.mktime(utc_return_nowtime(y).timetuple()) * 1000), mid])
			y += datetime.timedelta(days=1) 

	return json.dumps(new_time)



# 4 time call
#@cached(cache)
def get_aggregated_month(tag_id, get_time):
	new_time = []
	new_weeks=0
	allmonday_list = []
	weeks_list = []
	non_monday_weeks = []
	dframe = load_data(tag_id)
	df1 = datetime.datetime.strptime(get_time + ' 00:00:00', "%Y-%m-%d %H:%M:%S")
	df1 = time_tango(str(df1.year) + "-" + str(df1.month) + "-" + "01")
	b = 0
	non_monday_weeks=json.loads(new_week(tag_id,str(df1.date())))
	b = [non_monday_weeks[x][1] for x,y in enumerate(non_monday_weeks)]
	weeks_list.append(sum(b))
	
	for i in allmondays(df1.year,df1.month):
		week = json.loads(new_week(tag_id,str(i)))
		lst2 = [item[1] for item in week]
		weeks_list.append(sum(lst2))
	
	return json.dumps(util.parse_data(weeks_list,tag_id))


# 12 time call
def get_aggregated_year(tag_id,get_time):
	new_time = []
	new_weeks=0
	allmonday_list = []
	
	non_monday_weeks = []
	week_tuple=[]
	summed_month = []
	b = 0
	dframe = load_data(tag_id)
	for k in range(1,13):
		weeks_list = []
		df1 = datetime.datetime.strptime(get_time + ' 19:00:00', "%Y-%m-%d %H:%M:%S")
		df1 = time_tango(str(df1.year) + "-" + str(k) + "-" + "01")

		non_monday_weeks=json.loads(new_week(tag_id,str(df1.date())))
		b = [non_monday_weeks[x][1] for x,y in enumerate(non_monday_weeks)]
		weeks_list.append(sum(b))
		for i in allmondays(df1.year,df1.month):
			week = json.loads(new_week(tag_id,str(i)))
			lst2 = [item[1] for item in week]
			weeks_list.append(sum(lst2))
		summed_month.append(sum(weeks_list))
	return json.dumps(util.parse_data(summed_month,tag_id))

def get_infinite(tag_id,get_time):
	new_time = []
	new_weeks=0
	allmonday_list = []
	non_monday_weeks = []
	summed_month = []
	year_list = []
	dframe = load_data(tag_id)
	df1 = datetime.datetime.strptime(get_time + ' 19:00:00', "%Y-%m-%d %H:%M:%S")
	k=1
	while(k!=13):
		weeks_list = []
		df1 = time_tango(str(df1.year) + "-" + str(k) + "-" + "01")
		b = 0
		non_monday_weeks=json.loads(new_week(tag_id,str(df1.date())))
		b = [non_monday_weeks[x][1] for x,y in enumerate(non_monday_weeks)]
		weeks_list.append(sum(b))
		for i in allmondays(df1.year,df1.month):
			w = json.loads(new_week(tag_id,str(i)))
			lst2 = [item[1] for item in w]
			weeks_list.append(sum(lst2))
		summed_month.append(sum(weeks_list))
		k+=1
	year_list.append(sum(summed_month))
	return json.dumps(util.parse_data(year_list,tag_id))

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
	from_zone=tz.gettz('Asia/Karachi')
	to_zone =tz.tzlocal()
	return datetime.datetime.strptime(str(df), "%Y-%m-%d %H:%M:%S"). \
		replace(tzinfo=from_zone). \
		astimezone(to_zone)


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
	endtime=datetime.datetime.now()
	new_endtime=(int(time.mktime(utc_return(endtime).timetuple())) * 1000)
	return json.dumps(qb.query_time_bound_data(tag_id,new_endtime))

def last_day_of_month(any_day):
	next_month = any_day.replace(day=28) + datetime.timedelta(days=4)  # this will never fail
	return next_month - datetime.timedelta(days=next_month.day)

def first_monday(year,month):
	c = calendar.Calendar(firstweekday=calendar.SUNDAY)
	year = year; month = month
	monthcal = c.monthdatescalendar(year,month)
	first_monday = [day for week in monthcal for day in week if \
					day.weekday() == calendar.MONDAY and \
					day.month == month][0]
	return(first_monday)

def allmondays(year,month):
   d = date(year, month, 1)                    
   d += timedelta(days = 7 - d.weekday())  
   while d.month == month:
	  yield d
	  d += timedelta(days = 7)

@cached(cache)
def load_data(tag_id):
	alldata = json.loads(get_query_time_bound_data(tag_id))
	new_time = []
	datelist = []
	valuelist = []
	for i in range(len(alldata['tags'][0]['results'][0]['values'])):
		datelist.append(str(datetime.datetime.fromtimestamp(((alldata['tags'][0]['results'][0]['values'][i][0])+18000000)/1000).strftime('%Y-%m-%d')))
		valuelist.append(alldata['tags'][0]['results'][0]['values'][i][1])

	columns = ['Date', 'Values']
	dframe = pd.DataFrame(columns = columns)
	dframe['Date'] = datelist
	dframe['Date'] = pd.to_datetime(dframe['Date'])
	dframe['Values'] = valuelist 
	dframe['Date'] = pd.to_datetime(dframe['Date'])
	dframe['Date'] = dframe['Date'].dt.date	
	return dframe