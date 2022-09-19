import datetime
import os

import requests
from loguru import logger

import app_config
from rules.base_checker import *
from util.DSL import DSL


class UserCheck:
    def __init__(self, DbName):
        self.DbName = DbName
        self.curr_year = datetime.datetime.now().year
        self.root_path = os.path.dirname(__file__)
        self.dirPath = str(self.root_path[0:-5]) + "/static/csv"
        logger.remove()
        logger.add(str(self.root_path[0:-5]) + "/logs/log.log")
        self.logger = logger

    def get_item_list(self, id_list: list):
        """
        获取推荐用户详情
        :param DbName:
        :param id_list:
        :return:
        """
        query = {
            "query": {
                "terms": {"id": id_list}
            }
        }
        iterator = DSL(self.DbName).getES().search(index=app_config.es_user_index, body=query)
        result = []
        for item in iterator["hits"]["hits"]:
            # print(item["_source"])
            item['_source']["age"] = self.curr_year - int(str(item['_source'].get("birthday"))[0:4])
            result.append(item['_source'])

        return result

    def userCheck(self, recommendUrl, recommendBody):
        issue_map = {}
        ridList = []

        rep = requests.post(url=recommendUrl, data=json.dumps(recommendBody),
                            headers={"Content-Type": "application/json"})
        for item in rep.json()["itemList"]:
            ridList.append(item["id"])

        user_info = DSL(self.DbName).getId("members", recommendBody['uid'])["_source"]
        user_info["age"] = self.curr_year - int(str(user_info.get("birthday"))[0:4])
        rid_info = self.get_item_list(ridList)
        self.logger.info("推荐接口请求参数:" + str(recommendBody))
        self.logger.info("推荐接口请求结果:" + str(rep.json()))
        self.logger.info("推荐接口下发结果:" + str(ridList))

        issue_map["userAge"] = age_check(user_info, rid_info)
        issue_map["userSex"] = sex_check(user_info, rid_info)
        issue_map["userEdu"] = edu_check(user_info, rid_info)
        issue_map["userCity"] = city_check(user_info, rid_info)
        issue_map["userProvince"] = userProvince_check(user_info, rid_info)
        issue_map["userStatus"] = userStatus_check(user_info, rid_info)
        issue_map["userHeadStatus"] = userHeadStatus_check(user_info, rid_info)

        info = {}
        for user_fetch_fields in app_config.user_fetch_fields:
            info[user_fetch_fields] = user_info[user_fetch_fields]
        testResult = {"userInfo": info, "issue_map": issue_map, "ridList": ridList}
        return testResult
