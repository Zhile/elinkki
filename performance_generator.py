import random
from elasticsearch import Elasticsearch
import json
es = Elasticsearch("localhost:9200")
realtime_event_index = "realtime-event"


def get_device_count():
    body = '''
    {
  "aggs": {
    "device": {
      "terms": {
        "field": "deviceID"
      }
    }
  }, "size":0
}
    '''
    res = es.search(index=realtime_event_index, body=body)
    return res["aggregations"]["device"]["buckets"]


if __name__ == "__main__":
    res = get_device_count()
    months = ["2019-01", "2019-02"]
    result = dict()
    for r in res:
        key = r["key"]
        month_result = dict()
        for month in months:
            performance = random.randrange(80, 100)
            good_ratio = random.randrange(70, 100)
            ratio = dict()
            ratio["performance"] = performance/100.0
            ratio["good_ratio"] = good_ratio/100.0
            month_result[month] = ratio
        result[key] = month_result
    print(json.dumps(result))
