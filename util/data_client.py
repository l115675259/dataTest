import datetime
import json
import logging
import threading
import time
import traceback

import psycopg2
import requests
from requests.auth import HTTPBasicAuth

import app_config as config


class HttpClient:
    def __init__(self, base_url, token=None, **kwargs):
        self.base_url = base_url
        self.token = token
        self.kwargs = kwargs

    def request(self, method, url, params, **kwargs):
        if not kwargs.get("no_base", False):
            url = f"{self.base_url}/{url}"
        if "no_base" in kwargs:
            del kwargs["no_base"]
        real_kwargs = self.kwargs.copy()
        real_kwargs.update(kwargs)
        if self.token:
            if "headers" not in real_kwargs:
                real_kwargs["headers"] = {"Authorization": self.token}
            else:
                real_kwargs["headers"]["Authorization"] = self.token
        req = method(url, params, **real_kwargs)
        if req and req.status_code == 200:
            if len(str(req.content)) != "":
                return req.json()
            return ""
        else:
            raise Exception(f"数据获取异常：{req.text}")

    def post(self, url, params, **kwargs):
        headers = kwargs.get("headers", {})
        headers["Content-Type"] = "application/json"
        kwargs["headers"] = headers
        if isinstance(params, str):
            params = json.loads(params)
        s = requests.session()
        s.keep_alive = False
        return self.request(s.post, url, json.dumps(params), **kwargs)

    def get(self, url, params, **kwargs):
        if isinstance(params, str):
            params = json.loads(params)
        return self.request(requests.get, url, params, **kwargs)


class ESClient:
    def __init__(self, url, username, password):
        self.client = HttpClient(url, auth=HTTPBasicAuth(username, password))

    def search(self, index, body, scroll='5m', timeout='1m', size=1000):
        if body is None:
            body = {}
        params = body.copy()
        params["timeout"] = timeout
        params["size"] = size
        query_data = self.client.post(f"{index}/_search?scroll={scroll}", params)
        medata = query_data.get("hits").get("hits")
        if not medata:
            print('empty data')
            return []
        for item in medata:
            item["_source"]["_id"] = item["_id"]
            yield item["_source"]
        scroll_id = query_data["_scroll_id"]
        total = query_data["hits"]["total"]["value"]
        for i in range(int(total / size)):
            res = self.client.post("_search/scroll", {"scroll": scroll, "scroll_id": scroll_id})
            scroll_id = res["_scroll_id"]
            for item in res["hits"]["hits"]:
                item["_source"]["_id"] = item["_id"]
                yield item["_source"]

    def count(self, index, body):
        if body is None:
            body = {}
        params = body.copy()
        result = self.client.post(f"{index}/_count", params)
        return result.get("count")


class ApolloClient(object):
    def __init__(self, config_server_url, app_id, namespace="application", cluster='default', interval=60, ip=None):
        self.config_server_url = config_server_url
        self.appId = app_id
        self.cluster = cluster
        self.timeout = 60
        self.interval = interval
        self.init_ip(ip)
        self._stopping = False
        self._cache = {}
        self.default_namespace = namespace
        self._notification_map = {'application': -1}

    def init_ip(self, ip):
        if ip:
            self.ip = ip
        else:
            import socket
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.connect(('8.8.8.8', 53))
                ip = s.getsockname()[0]
            finally:
                s.close()
            self.ip = ip

    def get_value(self, key, default_val=None, namespace=None, auto_fetch_on_cache_miss=False):
        if not namespace:
            namespace = self.default_namespace
        if namespace not in self._notification_map:
            self._notification_map[namespace] = -1
            logging.getLogger(__name__).info("Add namespace '%s' to local notification map", namespace)

        if namespace not in self._cache:
            self._cache[namespace] = {}
            logging.getLogger(__name__).info("Add namespace '%s' to local cache", namespace)
            self._long_poll()
        if key in self._cache[namespace]:
            return self._cache[namespace][key]
        else:
            if auto_fetch_on_cache_miss:
                return self._cached_http_get(key, default_val, namespace)
            else:
                return default_val

    def start(self):
        if len(self._cache) == 0:
            self._long_poll()
        t = threading.Thread(target=self._listener)
        t.start()

    def stop(self):
        self._stopping = True
        logging.getLogger(__name__).info("Stopping listener...")

    def _cached_http_get(self, key, default_val, namespace='application'):
        url = '{}/configfiles/json/{}/{}/{}?ip={}'.format(self.config_server_url, self.appId, self.cluster, namespace,
                                                          self.ip)
        r = requests.get(url)
        if r.ok:
            data = r.json()
            self._cache[namespace] = data
            logging.getLogger(__name__).info('Updated local cache for namespace %s', namespace)
        else:
            data = self._cache[namespace]

        if key in data:
            return data[key]
        else:
            return default_val

    def _uncached_http_get(self, namespace='application'):
        url = '{}/configs/{}/{}/{}?ip={}'.format(self.config_server_url, self.appId, self.cluster, namespace, self.ip)
        r = requests.get(url)
        if r.status_code == 200:
            data = r.json()
            self._cache[namespace] = data['configurations']
            logging.getLogger(__name__).info('Updated local cache for namespace %s release key %s: %s',
                                             namespace, data['releaseKey'],
                                             repr(self._cache[namespace]))

    def _long_poll(self):
        url = '{}/notifications/v2'.format(self.config_server_url)
        notifications = []
        for key in self._notification_map:
            notification_id = self._notification_map[key]
            notifications.append({
                'namespaceName': key,
                'notificationId': notification_id
            })

        r = requests.get(url=url, params={
            'appId': self.appId,
            'cluster': self.cluster,
            'notifications': json.dumps(notifications, ensure_ascii=False)
        }, timeout=self.timeout)

        logging.getLogger(__name__).debug('Long polling returns %d: url=%s', r.status_code, r.request.url)

        if r.status_code == 304:
            # no change, loop
            logging.getLogger(__name__).debug('No change, loop...')
            return

        if r.status_code == 200:
            data = r.json()
            for entry in data:
                ns = entry['namespaceName']
                nid = entry['notificationId']
                logging.getLogger(__name__).info("%s has changes: notificationId=%d", ns, nid)
                self._uncached_http_get(ns)
                self._notification_map[ns] = nid
        else:
            logging.getLogger(__name__).warn('Sleep...')
            time.sleep(self.timeout)

    def _listener(self):
        logging.getLogger(__name__).info('Entering listener loop...')
        while not self._stopping:
            self._long_poll()
            time.sleep(self.interval)
        logging.getLogger(__name__).info("Listener stopped!")


class ItemType:
    DICT = 0
    OBJECT = 1
    FIRST = 2


def parse_res_val(res):
    if isinstance(res, datetime.datetime):
        val = str(res)
    elif isinstance(res, bytearray) or isinstance(res, bytes):
        val = res.decode('utf-8')
    else:
        val = res
    return val


class PgClient:

    def __init__(self, host, port, username, password, db, auto_commit=True):
        try:
            conn = psycopg2.connect(host=host, port=port, user=username, password=password, dbname=db)
            cursor = conn.cursor()
            self.conn = conn
            self.cursor = cursor
            self.auto_commit = auto_commit
            self._update = False
        except Exception as e:
            logging.warning("连接数据库异常:" + traceback.format_exc())
            raise Exception(e)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print("连接退出")
        if exc_tb:
            logging.error(f'type:{exc_type}')
            logging.error(f'value:{exc_val}')
            logging.error(f'trace:{exc_tb}')
            self.rollback()
            flag = False
        else:
            flag = True
            if not self.auto_commit and self._update:
                try:
                    self.commit()
                except Exception as e:
                    logging.error("事务提交失败：{}".format(str(e)))
                    self.rollback()
        if self.conn and self.conn != "":
            self.close()
        return flag

    def query(self, sql):
        if self.conn == "":
            return None
        self.cursor.execute(sql)
        res = self.cursor.fetchall()
        return res

    def query_list(self, sql, params=None, item_type: ItemType = ItemType.DICT):
        if self.conn:
            data_list = []
            self.cursor.execute(sql, params)
            res = self.cursor.fetchall()
            if not res:
                return data_list
            # 为结果集添加字段名信息
            columns = self.cursor.description
            for row in res:
                row_data = None
                if item_type == ItemType.DICT:
                    row_data = {}
                    for i in range(len(columns)):
                        row_data[columns[i][0]] = parse_res_val(row[i])
                elif item_type == ItemType.OBJECT:
                    row_data = []
                    for i in range(len(columns)):
                        row_data.append(parse_res_val(row[i]))
                elif item_type == ItemType.FIRST:
                    row_data = parse_res_val(row[0])
                if row_data:
                    data_list.append(row_data)
            return data_list
        return None

    def query_one(self, sql, params=None, item_type: ItemType = ItemType.DICT):
        if self.conn:
            self.cursor.execute(sql, params)
            res = self.cursor.fetchone()
            if not res:
                return None
            result = None
            if item_type == ItemType.DICT:
                result = {}
                # 为结果集添加字段名信息
                columns = self.cursor.description
                for i in range(len(columns)):
                    result[columns[i][0]] = parse_res_val(res[i])
            elif item_type == ItemType.OBJECT:
                result = []
                for item in res:
                    item_type.append(parse_res_val(item))
            elif item_type == ItemType.FIRST:
                result = parse_res_val(res[0])
            return result
        return None

    def commit(self):
        if self.conn != "":
            self.conn.commit()

    def close(self):
        if self.conn != "":
            self.cursor.close()
            self.conn.close()

    def rollback(self):
        if self.conn != "":
            self.conn.rollback()


# 初始化客户端
apollo_client = ApolloClient(config.apollo_url, config.apollo_id, config.apollo_namespace)
es_client = ESClient(config.es_url, config.es_username, config.es_password)
# pg_client = PgClient(**config.pg_conn)
# atexit.register(pg_client.__exit__, None, None, None)
