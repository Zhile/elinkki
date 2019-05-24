from elasticsearch import Elasticsearch
from elasticsearch import helpers
import csv
import uuid
import json

es = Elasticsearch(hosts="localhost:9200")
device_time_index = "device-time"
realtime_event_index = "realtime-event"
company_index = "company"


def bulk_docs_to_local(docs, index):
    success, _ = helpers.bulk(es, docs, index)
    print("Performed %d documents" % success)
    es.indices.flush(index=index)


def create_index(index_name, mapping_file):
    with open(mapping_file, "r") as mapping_file:
        mapping = mapping_file.read()
    es.indices.create(index=index_name, body=mapping)


def init_index():
    if not es.indices.exists(device_time_index):
        create_index(device_time_index, "./device_time_mapping.json")
    else:
        es.indices.delete(index=device_time_index)
        create_index(device_time_index, "./device_time_mapping.json")

    if not es.indices.exists(realtime_event_index):
        create_index(realtime_event_index, "./realtime_event_mapping.json")
    else:
        es.indices.delete(index=realtime_event_index)
        create_index(realtime_event_index, "./realtime_event_mapping.json")


def process_records(file, index):
    with open(file, "r") as csv_file:
        reader = csv.reader(csv_file, delimiter=",")
        line_count = 0
        header = list()
        docs = list()
        for row in reader:
            line_count += 1
            if line_count == 1:
                header = row
                continue
            doc = dict()
            doc["_id"] = uuid.uuid5(uuid.NAMESPACE_URL, json.dumps(row))
            doc["_index"] = index
            values = dict()
            for i in range(len(row)):
                if header[i] == "eventTime" and row[i] != "":
                    d = str(row[i].split(" ")[0])
                    year = d.split("/")[2]
                    month = d.split("/")[0]
                    if len(month) == 1:
                        month = "0"+month
                    day = d.split("/")[1]
                    if len(day) == 1:
                        day = "0" + day
                    values[header[i]] = year + "-" + month + "-" + day + " " + row[i].split(" ")[1]
                elif header[i] == "eventTime" and row[i] == "":
                    continue
                elif header[i] == "eventDetail":
                    values[header[i]] = row[i]
                    values["eventDetail_keyword"] = row[i]
                else:
                    values[header[i]] = row[i]

            doc["_source"] = json.dumps(values)
            docs.append(doc)
            if len(docs) % 1000 == 0:
                bulk_docs_to_local(docs=docs, index=index)
                docs.clear()
        if len(docs) != 0:
            bulk_docs_to_local(docs, index=index)

if __name__ == "__main__":
    init_index()
    device_time_file = "./device_time_with_header.csv"
    process_records(device_time_file, device_time_index)
    realtime_event_file = "./device_realtime_event_with_header.csv"
    process_records(realtime_event_file, realtime_event_index)
    company_info_file = "./company_info_with_header.csv"
    process_records(company_info_file, company_index)

