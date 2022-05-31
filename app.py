import json
import os

from flask import Flask, request
from loguru import logger

from util.DSL import DSL
from util.fileManage import myThread

app = Flask(__name__)

# 创建定时删除csv目录线程
# autoDeleteThread = myThread(1, "Thread-1", 1, 3, 20)
# autoDeleteThread.start()
# autoDeleteThread.join()

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


@app.route('/sqlQuery', methods=['POST', 'GET'])
def appPostSql():
    if request.method == 'POST':
        DbName = request.form["DbName"]
        sql = request.form["sql"]
        print(sql)
        dsl = DSL(DbName).postSql(sql)
        return dsl


@app.route('/test', methods=['POST', 'GET'])
def test():
    if request.method == 'POST':
        req = request.json
        if req["resultsToCsv"] == 0:
            dsl = DSL(req["DbName"]).postSql(req["sql"])
        else:
            dsl = DSL(req["DbName"]).queryResultsToCsv(req["sql"])
        return dsl


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002)
