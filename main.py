from flask import Flask
import query_service

app = Flask(__name__)


@app.route("/health", methods=['GET'])
def hello_world():
    return "The App is up!", 200

@app.route("/summary", methods=['GET'])
def get_summary():
    return query_service.get_summary()

if __name__ == "__main__":
    # app.run()
    # query_service.get_summary()
    query_service.get_oee_by_month("2019-01")



