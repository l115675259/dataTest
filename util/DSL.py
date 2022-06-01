import os
import sys
import uuid

import pandas
from elasticsearch import Elasticsearch
from flask import request
from loguru import logger

from util.config import Config


class DSL:

    def __init__(self, DbName):
        fn = getattr(sys.modules['__main__'], '__file__')
        self.root_path = os.path.dirname(__file__)

        logger.remove()
        logger.add(str(self.root_path[0:-5]) + "/logs/log.log")
        self.logger = logger

        self.DbName = DbName
        config = Config().getConsul()
        self.ESurl = config["elasticsearchConfig"][DbName]["url"]
        self.ESUser = config["elasticsearchConfig"][DbName]["userName"]
        self.ESPass = config["elasticsearchConfig"][DbName]["password"]

    def getES(self):
        try:
            esClient = Elasticsearch(self.ESurl,
                                     http_auth=(self.ESUser, self.ESPass)
                                     )

            if esClient.ping():
                self.logger.info("%s ES连接成功" % self.DbName)
                return esClient
            else:
                self.logger.info("%s ES连接失败" % self.DbName)
        except Exception as e:
            self.logger.info(e)

    def getId(self, indexName, ID):
        try:
            results = self.getES().get(index=indexName, doc_type="_doc", id=ID, request_timeout=20)
            self.logger.info("查询ID：" + str(ID))
            self.logger.info("查询结果：" + str(results))
            return results
        except Exception as e:
            self.logger.info(e)
            return "ES查询失败：" + str(e)

    def postSql(self, body):
        try:
            results = self.getES().sql.query(body={'query': body})

            self.logger.info("查询Sql：" + str(body))
            self.logger.info("查询结果：" + str(results))

            return results
        except Exception as e:
            self.logger.info(e)
            return "ES查询失败：" + str(e)

    def queryResultsToCsv(self, body):
        try:
            results = self.getES().sql.query(body={'query': body})
            self.logger.info("查询Sql：" + str(body))
            self.logger.info("查询结果：" + str(results))

            # es结果转DataFrame
            QrDf = pandas.DataFrame(results["rows"])

            # 从es结果获取列名并赋予
            columns = []
            for item in results["columns"]:
                columns.append(item["name"])
            QrDf.columns = columns

            # DataFrame转csv并保存
            fileName = str(self.root_path[0:-5]) + "/static/csv/" + str(uuid.uuid1()) + ".csv"
            QrDf.to_csv(fileName)

            # 获取hostname未运行flask则返回127
            try:
                hostname = request.headers.get('Host')
            except Exception:
                hostname = "127.0.0.1:5000"
            results["csvPath"] = "http://" + str(hostname) + "/" + str(
                fileName.split("/")[-3] + "/" + str(fileName.split("/")[-2] + "/" + str(fileName.split("/")[-1])))
            self.logger.info("csv访问地址：" + results["csvPath"])

            return results
        except Exception as e:
            self.logger.info(e)
            return "ES查询失败：" + str(e)

# if __name__ == '__main__':
#     # dsl = DSL("yd").getId("members", 121188118)
#     sql = "select id from members limit 100"
#     dsl = DSL("yd").queryResultsToCsv(sql)
#     # dsl = DSL("yd").postSql(sql)
#     print(dsl)
