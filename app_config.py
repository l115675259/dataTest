# 阿波罗连接配置
apollo_id = "BlogRecomService"
apollo_namespace = "app-conf-test"
apollo_url = "http://172.16.112.219:8080"

es_url = "http://172.16.225.160:9920"
es_username = "ydes_admin"
es_password = "eb_yd_1908"
es_blog_index = "mblog"
es_user_index = "members_hot"

# 用户规则检查点
user_rule_list = ["userSex", "userAge", "userProvinceNew", "userStatus", "userHeadStatus"]

# es获取用户信息字段
user_fetch_fields = ["id", "birthday", "sex", "province_id", "province", "city", "district", "education", "status",
                     "head_status", "active_at", "relationTerm", "living_status", "salary", "living_condition",
                     "marriage", "height"]
