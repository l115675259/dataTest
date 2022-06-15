import json

from rules.rule import checker
from util.data_client import apollo_client


@checker("age", "年龄范围")
def age_check(user_info: dict, item_list: list):
    age = user_info.get("age")
    age_conf = json.loads(apollo_client.get_value("testParam_1"))
    age_key = "maleAgeRangeJson" if user_info.get("sex") == 0 else "femaleAgeRangeJson"
    age_range_dict = age_conf.get(age_key)
    start_age = None
    end_age = None
    for age_range_key in age_range_dict:
        age_range_key: str
        start, end = age_range_key.split("-")
        start = int(start)
        end = int(end)
        if start <= age <= end:
            age_list = age_range_dict.get(age_range_key)
            if end > 50:
                start_age = start + age_list[0]
                end_age = end + age_list[1]
            else:
                start_age = age + age_list[0]
                end_age = age + age_list[1]
            break
    issue_list = []
    if start_age and end_age:
        for item in item_list:
            r_age = item.get("age")
            if r_age < start_age or r_age > end_age:
                issue_item = {"id": item.get("id"), "r_age": r_age}
                issue_list.append(issue_item)
    else:
        print(f"未检测到用户年龄配置")
    return f"{start_age}, {end_age}", issue_list


@checker("sex", "性别")
def sex_check(user_info: dict, item_list: list):
    sex = user_info.get("sex")
    issue_list = []
    if sex == 0:  # 女性不校验动态性别
        for item in item_list:
            if sex == item.get("sex"):
                issue_list.append(item.get("id"))
    return "男" if sex == 0 else "女", issue_list


@checker("edu", "学历")
def edu_check(user_info: dict, item_list: list):
    edu = user_info.get("education")
    issue_list = []
    if edu:
        lower_limit = 2 if user_info.get("sex") == 0 else 1
        edu_min = edu - lower_limit
        for item in item_list:
            u_edu = item.get("education")
            if u_edu and edu_min > u_edu:
                issue_item = {"id": item.get("id"), "education": item.get("education")}
                issue_list.append(issue_item)
    return edu, issue_list


@checker("city", "城市")
def city_check(user_info: dict, item_list: list):
    city = user_info.get("city")
    issue_list = []
    for item in item_list:
        if item.get("city") != city:
            issue_item = {"id": item.get("id"), "city": item.get("city")}
            issue_list.append(issue_item)
    return city, issue_list


@checker("userSex", "首页用户性别")
def userSex_check(user_info: dict, item_list: list):
    sex = user_info.get("sex")
    issue_list = []
    for item in item_list:
        if sex == item.get("sex"):
            issue_list.append(item.get("id"))
    return "男" if sex == 0 else "女", issue_list


@checker("userProvince", "首页用户省份")
def userProvince_check(user_info: dict, item_list: list):
    # 请求用户所在省份id
    user_provinceId = int(user_info.get("province_id"))
    # 字符串类型,请求用户的征友条件
    user_relationTerm = user_info.get("relationTerm")
    issue_list = []
    # 有征友条件，有值才转为json
    if user_relationTerm:
        # 字符串转为json类型
        user_relationTerm_json = json.loads(user_relationTerm)
        # 请求用户的征友省份id，注意：不确定某个属性是否有值则用get方式
        user_relationTerm_locationId = user_relationTerm_json.get('location_id')
        print(user_relationTerm_locationId)
        # 征友条件有值 且 有征友省份id
        if user_relationTerm and user_relationTerm_locationId:
            # 遍历推荐用户
            for item in item_list:
                # 征友省份为非全国
                if (user_relationTerm_locationId > 0):
                    # 推荐用户省份id ！= 当前登录用户的征友省份id
                    if (item.get("province_id") != user_relationTerm_locationId):
                        issue_item = {"_id": item.get("_id"), "province_id": item.get("province_id"),
                                      "living_status": item.get("living_status")}
                        issue_list.append(issue_item)
        # 征友条件有值 但 无征友省份id
        else:
            for item in item_list:
                if (item.get("province_id") != user_provinceId):
                    issue_item = {"_id": item.get("_id"), "province_id": item.get("province_id"),
                                  "living_status": item.get("living_status")}
                    issue_list.append(issue_item)
    # 无征友条件
    else:
        for item in item_list:
            if (item.get("province_id") != user_provinceId):
                issue_item = {"_id": item.get("_id"), "province_id": item.get("province_id"),
                              "living_status": item.get("living_status")}
                issue_list.append(issue_item)
    return user_provinceId, issue_list


@checker("userAge", "首页用户年龄")
def userAge_check(user_info: dict, item_list: list):
    # user_age = int(user_info.get("age"))
    user_relationTerm = user_info.get("relationTerm")
    start_age = None
    end_age = None
    issue_list = []
    # 有征友条件，有值才转为json
    if user_relationTerm:
        # 字符串转为json类型
        user_relationTerm_json = json.loads(user_relationTerm)
        # 请求用户的征友省份id，注意：不确定某个属性是否有值则用get方式
        startAge = user_relationTerm_json.get('start_age')
        endAge = user_relationTerm_json.get("end_age")
        # 征友条件有值 且 有征友起止年龄
        if user_relationTerm and startAge and endAge:
            print(f"推荐规则开始年龄：{startAge}，推荐规则结束年龄：{endAge}")
            for item in item_list:
                recomUser_age = item.get("age")
                if (recomUser_age < startAge or recomUser_age > (endAge + 1)):
                    issue_item = {"_id": item.get("_id"), "age": recomUser_age,
                                  "living_status": item.get("living_status")}
                    issue_list.append(issue_item)
        # 征友条件有值但无征友起止年龄
        else:
            userBasicAge_check(user_info, item_list)
    # 无征友条件
    else:
        userBasicAge_check(user_info, item_list)
    return f"{start_age}, {end_age}", issue_list


# 适用范围：无征友条件 或 有征友条件但无征友起止年龄
def userBasicAge_check(user_info: dict, item_list: list):
    # 当前登录用户的年龄
    user_age = int(user_info.get("age"))
    age_conf = json.loads(apollo_client.get_value("testParam_1"))
    age_key = "maleAgeRangeJson" if user_info.get("sex") == 0 else "femaleAgeRangeJson"
    # 获取阿波罗testParam_1的年龄配置
    age_range_dict = age_conf.get(age_key)
    start_age = None
    end_age = None
    for age_range_key in age_range_dict:
        age_range_key: str
        start, end = age_range_key.split("-")
        start = int(start)
        end = int(end)
        if (start <= user_age <= end):
            # 获取推荐年龄规则小几大几
            age_list = age_range_dict.get(age_range_key)
            # 推荐年龄规则的最小年龄
            start_age = user_age + age_list[0]
            # 推荐年龄规则的最大年龄
            end_age = user_age + age_list[1]
            break
    print(f"推荐规则后开始年龄：{start_age}，推荐规则后结束年龄：{end_age}")
    issue_list = []
    if start_age and end_age:
        for item in item_list:
            recomUser_age = item.get("age")
            if recomUser_age < start_age or recomUser_age > (end_age + 1):
                issue_item = {"_id": item.get("_id"), "age": recomUser_age, "living_status": item.get("living_status")}
                issue_list.append(issue_item)
    else:
        print(f"未检测到用户年龄配置")
    # return f"{start_age}, {end_age}", issue_list


@checker("userStatus", "首页用户状态")
def userStatus_check(user_info: dict, item_list: list):
    status = int(user_info.get("status"))
    issue_list = []
    for item in item_list:
        if item.get("status") != 1:
            issue_list.append(item.get("id"))
    return "正常" if status == 1 else "不正常", issue_list


@checker("userHeadStatus", "首页用户头像状态")
def userHeadStatus_check(user_info: dict, item_list: list):
    head_status = int(user_info.get("head_status"))
    issue_list = []
    for item in item_list:
        if item.get("head_status") != 1:
            issue_list.append(item.get("id"))
    return "正常" if head_status == 1 else "不正常", issue_list


# 用户在麦、在线、离线检查


@checker("userProvinceNew", "首页用户省份")
def userProvince_check(user_info: dict, item_list: list):
    # 请求用户所在省份id
    user_provinceId = int(user_info.get("province_id"))
    # 字符串类型,请求用户的征友条件
    user_relationTerm = user_info.get("relationTerm")
    issue_list = []
    for item in item_list:
        # 有征友条件，有值才转为json
        if user_relationTerm:
            # 字符串转为json类型
            user_relationTerm_json = json.loads(user_relationTerm)
            # 请求用户的征友省份id，注意：不确定某个属性是否有值则用get方式
            user_relationTerm_locationId = user_relationTerm_json.get('location_id')
            # print(user_relationTerm_locationId)
            # 征友条件为全国
            if (user_relationTerm_locationId == 0):
                break
            # 征友条件有值 且 有征友省份id
            if user_relationTerm and user_relationTerm_locationId:
                # 征友省份为非全国
                if (user_relationTerm_locationId > 0):
                    # 推荐用户省份id ！= 当前登录用户的征友省份id
                    if (item.get("province_id") != user_relationTerm_locationId):
                        issue_item = {"_id": item.get("_id"), "province_id": item.get("province_id"),
                                      "living_status": item.get("living_status")}
                        issue_list.append(issue_item)
            # 征友条件有值 但 无征友省份id
            else:
                if (item.get("province_id") != user_provinceId):
                    issue_item = {"_id": item.get("_id"), "province_id": item.get("province_id"),
                                  "living_status": item.get("living_status")}
                    issue_list.append(issue_item)
        # 无征友条件
        else:
            if (item.get("province_id") != user_provinceId):
                issue_item = {"_id": item.get("_id"), "province_id": item.get("province_id"),
                              "living_status": item.get("living_status")}
                issue_list.append(issue_item)
    return user_provinceId, issue_list
