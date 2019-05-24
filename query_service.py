import json

from elasticsearch import Elasticsearch

es = Elasticsearch("localhost:9200")
device_time_index = "device-time"
realtime_event_index = "realtime-event"
company_index = "company"

with open("./total_hour_query.json", "r") as total_hour_query_file:
    total_hour_query = total_hour_query_file.read()
with open("./performance.json") as performance_file:
    random_performance = json.loads(performance_file.read())

def query_total_hour():
    res = es.search(index=device_time_index, body=total_hour_query)
    return res


def get_device_hour():
    result = list()
    res = query_total_hour()
    device_bucket = res["aggregations"]["avg_hour"]["buckets"]
    if len(device_bucket) == 0:
        return result
    for e in device_bucket:
        deviceID = e["key"]
        if deviceID == "":
            continue
        sum_result = e["hour_sum"]["buckets"]
        values = list()
        for s in sum_result:
            month = s["key_as_string"]
            total_running_hour = s["total_running_hour"]["value"]
            total_standby_hour = s["total_standby_hour"]["value"]
            total_warning_hour = s["total_warning_hour"]["value"]
            total_offline_hour = s["total_offline_hour"]["value"]
            value = dict()
            value["month"] = month
            value["total_running_hour"] = total_running_hour
            value["total_standby_hour"] = total_standby_hour
            value["total_warning_hour"] = total_warning_hour
            value["total_offline_hour"] = total_offline_hour
            values.append(value)
        one_device = dict()
        one_device["deviceID"] = deviceID
        one_device["values"] = values
        result.append(one_device)
    return result


def get_ratio():
    device_hours = get_device_hour()
    for device in device_hours:
        for value in device["values"]:
            total_standby_hour = value["total_standby_hour"]
            total_warning_hour = value["total_warning_hour"]
            total_offline_hour = value["total_offline_hour"]
            total_running_hour = value["total_running_hour"]
            standby_ratio = total_standby_hour / (total_standby_hour + total_warning_hour + total_offline_hour)
            break_ratio = total_warning_hour / (total_standby_hour + total_warning_hour + total_offline_hour)
            offline_ration = total_offline_hour / (total_standby_hour + total_warning_hour + total_offline_hour)
            time_ratio = total_running_hour / (
                        total_running_hour + total_standby_hour + total_warning_hour + total_offline_hour)
            value["standby_ratio"] = standby_ratio
            value["break_ratio"] = break_ratio
            value["offline_ratio"] = offline_ration
            value["time_ratio"] = time_ratio
            month = value["month"]
            value["performance"] = random_performance[device["deviceID"]][month]["performance"]
            value["good_ratio"] = random_performance[device["deviceID"]][month]["good_ratio"]
            value["oee"] = value["time_ratio"] * value["performance"] * value["good_ratio"]
    return device_hours


def get_break_count():
    body = '''
    {
  "aggs": {
    "filter_device": {
      "filter": {
        "term": {
          "eventDetail": "故障"
        }
      },
      "aggs": {
        "device": {
          "terms": {
            "field": "deviceID"
          }
        }
      }
    }
  },
  "size": 0
}
    '''
    res = es.search(index=realtime_event_index, body=body)
    buckets = res["aggregations"]["filter_device"]["device"]["buckets"]
    result = dict()
    for r in buckets:
        result[r["key"]] = r["doc_count"]
    return result


def get_device_count():
    body = '''
    {
  "aggs": {
    "device_count": {
      "cardinality": {
        "field": "deviceID"
      }
    }
  }
}
    '''
    res = es.search(index=realtime_event_index, body=body)
    return res["aggregations"]["device_count"]["value"]


def get_event_count():
    body = '''
    {
  "query": {
    "match_all": {}
  }
}
    '''
    res = es.search(index=realtime_event_index, body=body)
    return res["hits"]["total"]["value"]


def get_break_count():
    body = '''
    {
  "query": {
    "term": {
      "eventDetail": "故障"
    }
  }
}   
    '''
    res = es.search(index=realtime_event_index, body=body)
    return res["hits"]["total"]["value"]


def get_break_category():
    body = '''
    {
  "aggs": {
    "filter_aggs": {
      "filter": {
        "term": {
          "eventDetail": "故障"
        }
      },
      "aggs": {
        "event_type": {
          "terms": {
            "field": "eventDetail_keyword",
            "size": 100
          }
        }
      }
    }
  },
  "size": 0
}
    '''
    res = es.search(index=realtime_event_index, body=body)
    buckets = res["aggregations"]["filter_aggs"]["event_type"]["buckets"]
    result = dict()
    for r in buckets:
        result[r["key"]] = r["doc_count"]
    return result


def get_break_count_by_month():
    body = '''
    {
  "aggs": {
    "filter_aggs": {
      "filter": {
        "term": {
          "eventDetail": "故障"
        }
      },
      "aggs": {
        "month_bucket": {
          "date_histogram": {
            "field": "eventTime",
            "interval": "month",
            "format": "yyyy-MM"
          }
        }
      }
    }
  },
  "size": 0
}
    '''
    res = es.search(index=realtime_event_index, body=body)
    buckets = res["aggregations"]["filter_aggs"]["month_bucket"]["buckets"]
    result = dict()
    for r in buckets:
        result[r["key_as_string"]] = r["doc_count"]
    return result


def get_total_hour_by_day():
    body = '''
{
  "aggs": {
    "hour_sum": {
      "date_histogram": {
        "field": "eventTime",
        "interval": "day",
        "format": "yyyy-MM-dd"
      },
      "aggs": {
        "total_warning_hour": {
          "sum": {
            "field": "warningHour"
          }
        },
        "total_offline_hour": {
          "sum": {
            "field": "offlineHour"
          }
        },
        "total_standby_hour": {
          "sum": {
            "field": "standbyHour"
          }
        }
      }
    }
  },
  "size": 0
}
    '''
    res = es.search(index=device_time_index, body=body)
    buckets = res["aggregations"]["hour_sum"]["buckets"]
    result = dict()
    print(res)
    for r in buckets:
        total_hour = dict()
        total_hour["total_standby_hour"] = r["total_standby_hour"]["value"]
        total_hour["total_offline_hour"] = r["total_offline_hour"]["value"]
        total_hour["total_warning_hour"] = r["total_warning_hour"]["value"]
        result[r["key_as_string"]] = total_hour
    return result


def get_summary():
    all_device_count = get_device_count()
    all_event_count = get_event_count()
    all_break_count = get_break_count()
    break_category = get_break_category()
    res = get_ratio()
    total_running_hour = 0
    total_break_hour = 0
    mtbf = dict()
    mttr = dict()
    for device in res:
        for value in device["values"]:
            total_running_hour += value["total_running_hour"]
            total_break_hour += value["total_warning_hour"]
            if value["month"] not in mtbf:
                mtbf[value["month"]] = value["total_running_hour"]
            else:
                mtbf[value["month"]] += value["total_running_hour"]
            if value["month"] not in mttr:
                mttr[value["month"]] = value["total_warning_hour"]
            else:
                mttr[value["month"]] += value["total_warning_hour"]
    all_mtbf = total_running_hour / all_break_count
    all_mttr = total_break_hour / all_break_count
    total_break_by_month = get_break_count_by_month()
    mtbf_by_month = dict()
    mttr_by_month = dict()
    for month in total_break_by_month:
        mtbf_by_month[month] = mtbf[month] / total_break_by_month[month]
        mttr_by_month[month] = mttr[month] / total_break_by_month[month]
    total_hour = get_total_hour_by_day()

    result = dict()
    result["all_device_count"] = all_device_count
    result["all_event_count"] = all_event_count
    result["all_break_count"] = all_break_count
    result["all_mtbf"] = all_mtbf
    result["all_mttr"] = all_mttr
    result["break_category"] = break_category
    result["mtbf_by_month"] = mtbf_by_month
    result["mttr_by_month"] = mttr_by_month
    result["total_hour_by_day"] = total_hour
    print(json.dumps(result))

def get_device_name():
    body = '''
    {
  "aggs": {
    "device": {
      "terms": {
        "field": "deviceID"
      },
      "aggs": {
        "name": {
          "terms": {
            "field": "deviceName"
          }
        }
      }
    }
  }
}
    
    '''
    res = es.search(index=device_time_index, body=body)
    buckets = res["aggregations"]["device"]["buckets"]
    result = dict()
    for r in buckets:
        device_id = r["key"]
        device_name = r["name"]["buckets"][0]["key"]
        result[device_id] = device_name
    return result


def get_detailed_oee():
    res = get_ratio()
    names = get_device_name()
    for r in res:
        name = names[r["deviceID"]]
        r["name"] = name
    return res

def get_oee_by_month(month):
    res = get_detailed_oee()
    for item in res:
        values = item["values"]
        for value in values:
            if value["month"] == month:
                new_values = list()
                new_values.append(value)
                item["values"] = new_values
                break
    return res

def get_overall_oee():
    res = get_detailed_oee()
    for item in res:
        total_running_hour = 0
        total_standby_hour = 0
        total_warning_hour = 0
        total_offline_hour = 0
        performance = 0
        good_ratio = 0
        values = item["values"]
        for value in values:
            total_running_hour += values["total_running_hour"]
            total_standby_hour += values["total_standby_hour"]
            total_warning_hour += values["total_warning_hour"]
            total_offline_hour += values["total_offline_hour"]
            performance += values["performance"]
            good_ratio += values["good_ratio"]