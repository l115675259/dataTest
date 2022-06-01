from flask import Flask, request

from util.DSL import DSL
from util.fileManage import FileManage

app = Flask(__name__)


# root_path = os.path.dirname(__file__)
# logger.remove()
# logger.add(str(root_path) + "/logs/log.log")
# logger = logger


@app.route('/idQuery', methods=['POST', 'GET'])
def appGetId():
    if request.method == 'POST':
        DbName = request.form["DbName"]
        index = request.form["index"]
        uid = request.form["uid"]
        # logger.info("Request From:" + str(request))
        dsl = DSL(DbName).getId(index, uid)

        return dsl


# @app.route('/sqlQuery', methods=['POST', 'GET'])
# def appPostSql():
#     if request.method == 'POST':
#         DbName = request.form["DbName"]
#         sql = request.form["sql"]
#         print(sql)
#         dsl = DSL(DbName).postSql(sql)
#         return dsl


@app.route('/sqlQuery', methods=['POST', 'GET'])
def appPostSql():
    if request.method == 'POST':
        req = request.json
        if req["resultsToCsv"] == 0:
            dsl = DSL(req["DbName"]).postSql(req["sql"])
        else:
            dsl = DSL(req["DbName"]).queryResultsToCsv(req["sql"])
        return dsl


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002)
