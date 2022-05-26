import json
import os
import sys

from elasticsearch import Elasticsearch

from loguru import logger

from util.config import Config


class DSL:

    def __init__(self, DbName):
        fn = getattr(sys.modules['__main__'], '__file__')
        root_path = os.path.dirname(__file__)

        logger.remove()
        logger.add(str(root_path[0:-5]) + "/logs/log.log")
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
            results = str(self.getES().sql.query(body={'query': body}))

            self.logger.info("查询Sql：" + str(body))
            self.logger.info("查询结果：" + str(results))

            return results
        except Exception as e:
            self.logger.info(e)
            return "ES查询失败：" + str(e)

# if __name__ == '__main__':
#     dsl = DSL().getId("members", 121188119)
#     print(dsl)
