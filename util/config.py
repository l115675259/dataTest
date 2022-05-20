import json
import os
import sys

import consul
import nacos

from loguru import logger


class Config:

    def __init__(self):
        fn = getattr(sys.modules['__main__'], '__file__')
        root_path = os.path.dirname(__file__)
        logger.remove()
        logger.add(str(root_path[0:-5]) + "/logs/log.log")
        self.logger = logger
        self.SERVER_ADDRESSES = "172.16.227.63"
        self.port = 5100

    def getConfig(self):
        try:
            SERVER_ADDRESSES = "192.168.2.2:8848"  # nacos的ip:port
            NAMESPACE = "cec3b813-17c7-4cf3-80b4-c729065ac32e"  # 命名空间的id: namespace id
            client = nacos.NacosClient(SERVER_ADDRESSES, namespace=NAMESPACE, username="nacos", password="nacos")
            service_info = client.get_config("elasticSearchTest", "DEFAULT_GROUP")
            if service_info:
                elasticSearchTestConfig = client.get_config("elasticSearchTest", "DEFAULT_GROUP")
                return json.loads(elasticSearchTestConfig)
            else:
                return "未找到服务信息"
        except Exception as e:
            self.logger.info(f"获取服务ip异常: {e}")
            raise

    def getConsul(self):
        try:
            client = consul.Consul(host='172.16.227.63', port=5100, scheme='http')
            server_config = json.loads(client.kv.get("elasticSearchTest")[1]['Value'].decode('utf-8'))
            self.logger.info(server_config)
            return server_config
        except Exception as e:
            self.logger.info(f"获取服务配置ip异常: {e}")
