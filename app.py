from flask import Flask, request

from util.DSL import DSL
from work.userCheck import UserCheck

app = Flask(__name__)


@app.route('/idQuery', methods=['POST', 'GET'])
def appGetId():
    if request.method == 'POST':
        DbName = request.form["DbName"]
        index = request.form["index"]
        uid = request.form["uid"]
        dsl = DSL(DbName).getId(index, uid)

        return dsl


@app.route('/sqlQuery', methods=['POST', 'GET'])
def appPostSql():
    if request.method == 'POST':
        req = request.json
        if req["resultsToCsv"] == 0:
            dsl = DSL(req["DbName"]).postSql(req["sql"])
        else:
            dsl = DSL(req["DbName"]).queryResultsToCsv(req["sql"])
        return dsl


@app.route('/keyTest', methods=['POST', 'GET'])
def appKeyTest():
    if request.method == 'POST':
        rep = request.json
        dsl = UserCheck("yd").userCheck(rep["recommendUrl"], rep["recommendBody"])

        return dsl


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002)
