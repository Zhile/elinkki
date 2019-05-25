import json

from flask import Flask
from flask import request
import query_service

app = Flask(__name__)


@app.route("/health", methods=['GET'])
def hello_world():
    return "The App is up!", 200


@app.route("/summary", methods=['GET'])
def get_summary():
    return query_service.get_summary()


@app.route("/performance", methods=['GET'])
def get_performance():
    month = request.form['month']
    status = request.form['status']
    if month is None and status is None:
        return query_service.get_overall_oee()
    elif status is None:
        return query_service.get_oee_by_month(month)
    elif month is None:
        return query_service.get_oee_by_status(status)
    else:
        res = query_service.get_oee_by_month(month)
        return query_service.get_oee_by_month_status(res, status)


@app.route("/device/<device_id>", methods=['GET'])
def get_device_detail(device_id):
    query_service.get_device_detail(device_id)


if __name__ == "__main__":
    app.run()
    # query_service.get_summary()
    # print(json.dumps(query_service.get_oee_by_month("2019-01")))
    # print(json.dumps(query_service.get_oee_by_status("<50%")))
    # print(json.dumps(query_service.get_overall_oee()))
    # res = query_service.get_oee_by_month("2019-02")
    # print(json.dumps(query_service.get_oee_by_month_status(res, "<50%")))
    # print(json.dumps(query_service.get_device_detail("115099")))


