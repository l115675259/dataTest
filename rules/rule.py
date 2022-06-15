# 规则检查字典
import os

from loguru import logger

rules_map = {}


def checker(rule_key: str, key_desc: str):
    """
    装饰器 加载规则，存入map
    :param key_desc:
    :param rule_key:
    :return:
    """
    spec = 64
    limit = 100

    def wrapper(func):
        root_path = os.path.dirname(__file__)
        dirPath = str(root_path[0:-5]) + "/static/csv"
        logger.remove()
        logger.add(str(root_path[0:-5]) + "/logs/log.log")

        def wrapper_func(*args, **kwargs):
            logger.info(("=" * spec + f" 开始检查：{rule_key} " + "=" * spec)[0:limit])
            user_info = args[0]
            feature, issue_list = func(*args, **kwargs)
            success = True if not issue_list else False
            logger.info(f"用户：<{user_info.get('id')}>  {key_desc}：<{feature}>")
            status = "\033[1;32m 通过 \033[0m" if success else "\033[1;31m 未通过 \033[0m"
            err_num = len(issue_list)
            err_num = str(err_num) if success else f"\033[1;31m{err_num}\033[0m"
            message = "检测到 " + err_num + " 条异常数据，" + status
            if not success:
                message += "，详情如下："
            logger.info(message)
            if issue_list:
                for issue_item in issue_list:
                    logger.info(f"{key_desc} 异常：{issue_item}")
            logger.info(("=" * spec + f" 检查结束：{rule_key} " + "=" * spec)[0:limit])
            return issue_list

        rules_map[rule_key] = wrapper_func
        return wrapper_func

    return wrapper
