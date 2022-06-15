import json
import time

from utils.data_client import pg_client, apollo_client, ItemType

from rules.rule import checker

flow_conf: dict = json.loads(apollo_client.get_value("sameCityFlowControl"))
hot_ctr_ids = {int(i) for i in flow_conf.get("热度对照组").split(",")}
hot_abs_ids = {int(i) for i in flow_conf.get("热度实验组").split(",")}


@checker("repeated", "重复ID")
def repeated_check(user_info: dict, item_list: list):
    ids = set()
    issue_list = []
    total = len(item_list)
    for item in item_list:
        ids.add(item.get("_id"))
    if total != len(ids):
        issue_list.append("严重：id重复！")
    return None, issue_list


@checker("user", "用户隔离")
def user_check(user_info: dict, item_list: list):
    issue_list = []
    last_uid = None
    same_content_type_list = []
    for item in item_list:
        curr_uid = item.get("uid")
        if last_uid != curr_uid:
            if same_content_type_list and len(same_content_type_list) > 1:
                issue_item = {"uid": last_uid, "blog_list": ",".join(same_content_type_list)}
                issue_list.append(issue_item)
            same_content_type_list.clear()
            same_content_type_list.append(item.get("_id"))
            last_uid = curr_uid
        else:
            same_content_type_list.append(item.get("_id"))
    # 最后结果校验
    if same_content_type_list and len(same_content_type_list) > 1:
        issue_item = {"uid": last_uid, "blog_list": ",".join(same_content_type_list)}
        issue_list.append(issue_item)
    return None, issue_list


@checker("content", "内容类型隔离")
def content_check(user_info: dict, item_list: list):
    issue_list = []
    last_content_type = None
    same_content_type_list = []
    for item in item_list:
        curr_content_type = item.get("content_type")
        if last_content_type != curr_content_type:
            if same_content_type_list and len(same_content_type_list) > 1:
                issue_item = {"content_type": last_content_type, "blog_list": ",".join(same_content_type_list)}
                issue_list.append(issue_item)
            same_content_type_list.clear()
            same_content_type_list.append(item.get("_id"))
            last_content_type = curr_content_type
        else:
            same_content_type_list.append(item.get("_id"))
    # 最后结果校验
    if same_content_type_list and len(same_content_type_list) > 1:
        issue_item = {"content_type": last_content_type, "blog_list": ",".join(same_content_type_list)}
        issue_list.append(issue_item)
    return None, issue_list


@checker("hot", "用户热度")
def hot_ahead_check(user_info: dict, item_list: list):
    issue_list = []
    first_blog_id = item_list[0].get("_id")
    uid_suffix = int(user_info.get("_id")) % 10
    tab_flag = None
    sex_flag = "man" if user_info.get("sex") == 0 else "woman"
    if uid_suffix in hot_abs_ids:
        tab_flag = "absolute_value"
    elif uid_suffix in hot_ctr_ids:
        tab_flag = "ctr"
    ids_str = ",".join([f"'{item.get('_id')}'" for item in item_list])
    table_name = f"dynamic_heat_{tab_flag}_{sex_flag}"
    sql = f"select blogid,score from ydrelation.{table_name} where blogid in ({ids_str})"
    score_list = pg_client.query_list(sql)
    score_list.sort(key=lambda item: item.get("score"), reverse=True)
    try:
        hot_max_id = score_list[0].get("blogid")
        if first_blog_id != hot_max_id:
            issue_list.append({
                "error": "热度未置顶",
                "first_id": first_blog_id,
                "hot_max_id": hot_max_id
            })
            for item in score_list:
                issue_list.append({
                    "id": item.get("blogid"),
                    "score": item.get("score")
                })
    except IndexError:
        issue_list.append("严重：缺少热度数据！")
    return (tab_flag, sex_flag), issue_list


@checker("tag", "标签数量")
def tag_check(user_info: dict, item_list: list):
    issue_list = []
    rule_dict: dict = user_info["rank_rule"]
    count = sum([v for k, v in rule_dict.items() if k != "tag"])
    limit = 10 - count
    tag_rule = rule_dict.get("tag")
    tag_dict = {}
    for item in item_list:
        f_tag = item.get("b_ftag")
        if f_tag is not None:
            limit -= 1
            tag_dict[f_tag] = tag_dict.get(f_tag, []) + [item.get("_id")]
            continue
    if limit > 0:
        issue_list.append("标签不足")
        for k, v in tag_dict.items():
            issue_list.append({k: v})
    return tag_rule, issue_list


@checker("new", "新内容")
def new_check(user_info: dict, item_list: list):
    issue_list = []
    curr_time = int(time.time())
    curr_time = time.localtime(curr_time)
    curr_time = int(time.strftime("%Y%m%d%H%M%S", curr_time))
    yesterday_time = curr_time - 1000000 * 2
    rule_dict: dict = user_info["rank_rule"]
    blog_new_num = rule_dict.get("blogNew")
    new_list = []
    for item in item_list:
        item_time = item.get("created_at")
        if item_time >= yesterday_time:
            new_list.append(item)
    if len(new_list) < blog_new_num:
        issue_list.append("严重：新内容数据不足！")
        issue_list.extend(new_list)
    quality_rate = False
    for item in new_list:
        if item.get("quality_rate") == 1:
            quality_rate = True
            break
    if not quality_rate:
        issue_list.append("缺少优质内容！")
    return f"新内容：{blog_new_num}条", issue_list


@checker("editor", "小编动态")
def status_check(user_info: dict, item_list: list):
    issue_list = []
    editor_ids = update_editor()
    for item in item_list:
        if item.get("uid") in editor_ids:
            issue_list.append({"id": item.get("_id"), "uid": item.get("uid"), "topicId": item.get("topic_id")})
    if not issue_list:
        issue_list.append("话题数据关联小编为空！")
    return f"小编用户量：{len(editor_ids)}", issue_list


def update_editor():
    sql = "select id from ydrelation.ods_db_yidui_uo_uo_atmosphere_members_all"
    return pg_client.query_list(sql, item_type=ItemType.FIRST)


if __name__ == '__main__':
    ids = update_editor()
    print(ids)
