import json
import inspect


def get_subtraction(data_dict1, data_dict2):
    return [[x1[0], x1[1] - x2[1]] for (x1, x2) in zip(data_dict1, data_dict2)]


def get_perc_conversion(data_values1, data_values2, data_values3):
    return [[x1[0], (x1[1] - x2[1]) / x3[1]] for (x1, x2, x3) \
            in zip(data_values1, data_values2, data_values3)]


def parse_data_reon(data, tag=None):
    format = {"response": {'tag': None, 'results': []}}
    if inspect.stack()[1][3] == 'get_aggregated_day':
        for i in data:
            format['response']['tag'] = i['tags'][0]['name']
            format['response']['results']. \
                append(i['tags'][0]['results'][0]['values'][0]
                       if len(i['tags'][0]['results'][0]['values']) != 0 else [])
        return json.dumps(format)
    elif inspect.stack()[1][3] == 'get_aggregated_month' or \
            inspect.stack()[1][3] == 'get_aggregated_week':
        format['response']['tag'] = tag
        format['response']['results'] = data
        return json.dumps(format)
    elif inspect.stack()[1][3] == 'get_aggregated_year':
        copyData = list(data)
        copyData.reverse()
        format['response']['tag'] = tag
        format['response']['results'] = copyData
        return json.dumps(format)

def parse_data(data,tag):
    res=[]
    json_object={}
    tem={}
    for key,i in enumerate(data):
        res.append(i)
    json_object.update({"results": res})
    tem.update({"response":json_object})
    tem['response']['tag']=tag
    return (tem)



def parse_data_zeroslen(data):
    return {
        'tag': data["tags"][0].get('name'),
        'results': len(data["tags"][0]["results"][0]["values"])
    }
