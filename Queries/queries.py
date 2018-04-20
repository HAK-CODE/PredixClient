def aggregated_body(tag_id, start_time, end_time, time_sp, val, type_inp):
	return {
		"tags": [
			{
				"name": tag_id,
				"order": "desc",
				"aggregations": [
					{
						"type": type_inp,
						"sampling": {
							"unit": time_sp,
							"value": val
						}
					}
				]
			}
		],
		"start": start_time,
		"end": end_time
	}


def real_value(tag_id):
	return {
		"tags": [
			{
				"name": tag_id
			}
		]
	}


def zero_count(tag_id):
	return {
		"start": "1d-ago",
		"tags": [
			{
				"name": tag_id,
				"order": "desc",
				"filters": {"measurements": {"values": 0, "condition": "eq"}},
				"limit": 10000000
			}
		]
	}


def time_bound_data(tag_id,endtime):
	return {
		"cache_time": 0,
		"tags": [
			{
				"name": tag_id,
				"order": "desc"
			}
				],
		"start": 1522522800000,
		"end": endtime

	}


def year_count(tag_id):
	return {
		"start": "1y-ago",
		"tags": [
			{
				"name": tag_id,
				"order": "desc",
				"limit": 12
			}
		]
	}

def real_dayvalue(tag_id,time,end_time):
    return {
        "start": time,
        "end":end_time,
        "tags": [
            {
        "name": tag_id,
        "order": "desc",
        }
    ]
    }