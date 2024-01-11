import logging
import re
import time

import pymysql
import redis


class Redis2Mysql:
    def __init__(self,
                 mysql_host: str,
                 mysql_port: int,
                 mysql_user: str,
                 mysql_password: str,
                 mysql_database: str,
                 redis_host: str,
                 redis_port: int,
                 redis_db: int,
                 batch_size: int,
                 table_structure_list: list):
        self.batch_size = batch_size
        # 转化table_structure_list
        self.db_name_list = []  # 定义数据表名
        self.table_structures = []  # 这是一个可变数量参数的变量

        for table_structure in table_structure_list:
            name = table_structure['name']
            self.db_name_list.append(name)
            fields = table_structure['fields']
            self.table_structures.append(fields)
        self.table_structures = tuple(self.table_structures)  # 变成可变参数
        logging.debug(f"\033[34m======self.db_name_list: {self.db_name_list}======\033[0m")
        logging.debug(f"\033[34m======self.table_structures: {self.table_structures}======\033[0m")

        # ------------batch_sync_controller 不可动!!!!!!!!!!!!!!!!------------
        try:
            # ------------初始化Redis------------
            self.redis_connection = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db)
            logging.info(f"\033[32m====Redis Connection Info====\n{self.redis_connection}\033[0m")
        except Exception as redis_error:
            logging.error(f"\033[31m====Failed to initialize Redis connection: {redis_error}\033[0m")

        try:
            # ------------初始化Mysql------------
            self.mysql_connection = pymysql.connect(host=mysql_host, port=mysql_port, user=mysql_user,
                                                    password=mysql_password, database=mysql_database)
            logging.info(f"\033[32m====Mysql Connection Info====\n{self.mysql_connection}\033[0m")
            self.mysql_cursor = self.mysql_connection.cursor()
            logging.debug(f"\033[34m====self.mysql_cursor====\n{self.mysql_cursor}\033[0m")
        except Exception as mysql_error:
            logging.error(f"\033[31m====Failed to initialize Mysql connection: {mysql_error}\033[0m")

    def sync_redis_2_mysql(self):
        try:
            redis_results = self.scan_keys(pattern='*', count=self.batch_size)
            # redis_results = redis_results[:2]

            logging.debug(f"\033[32m==redis_results: {redis_results}==\033[0m")

            for key in redis_results:
                key = key.decode('utf-8')
                logging.info(f"\033[32m====QUERY KEY====\n{key}\033[0m")
                # 获取Redis键的前缀
                prefix = key.split(':')[0]

                self.batch_sync_controller(key, prefix, self.db_name_list, self.table_structures)

            time.sleep(5)
        except pymysql.Error as mysql_error:
            logging.error(f"\033[31m====Mysql Error====\n{mysql_error}\033[0m")
        except redis.RedisError as redis_error:
            logging.error(f"\033[31m====Redis Error====\n{redis_error}\033[0m")
        finally:
            # 关闭 MySQL 连接和游标
            if 'mysql_cursor' in locals() and self.mysql_cursor is not None:
                self.mysql_cursor.close()
            if 'mysql_connection' in locals() and self.mysql_connection is not None:
                self.mysql_connection.close()
            # 关闭 Redis 连接
            if 'redis_connection' in locals() and self.redis_connection is not None:
                self.redis_connection.close()

    def batch_sync_controller(self, key: str, prefix: str, db_name_list: list, *table_structures):
        logging.info("\033[32m====Entering batch_sync_controller====\033[0m")
        value = self.redis_connection.hgetall(key)
        # --------解码所有键值对--------
        value = {k.decode('utf-8'): v.decode('utf-8') for k, v in value.items()}
        logging.info(f"\033[32m====redis value====\n{value}\033[0m")

        for db_name, table_structure in zip(db_name_list, *table_structures):
            # 数据去杂：使用数据校验模块即可判断是否需要处理
            if not self.data_validate(value):
                redis_pipe = self.redis_connection.pipeline()  # 获取 Redis 客户端对象
                redis_pipe.multi()  # 开启事务
                redis_pipe.delete(key)
                redis_result = redis_pipe.execute()
                logging.info(f"\033[32m====Null answer, Redis row deleted:{redis_result}====\033[0m")
                continue  # 直接进入下一次迭代
            # 匹配到需要处理的表
            pattern = re.compile(f'.*{re.escape(prefix)}.*')
            if pattern.match(db_name):

                # TODO 数据去重：使用数据去重模块，对数据库的case_url进行一遍查重，看看是否匹配到了相同的url，如果相同，那就返回False
                if self.is_url_duplicate(db_name, value, table_structure) > 0:
                    redis_pipe = self.redis_connection.pipeline()  # 获取 Redis 客户端对象
                    redis_pipe.multi()  # 开启事务
                    redis_pipe.delete(key)
                    redis_result = redis_pipe.execute()
                    logging.info(f"\033[32m====Url Duplicated, Redis row deleted:{redis_result}====\033[0m")
                    continue  # 直接进入下一次迭代

                logging.info(f"\033[32m====Processing====\n{db_name}\n====table_structure====\n{table_structure}")
                # ------------------------组装SQL区------------------------
                # 需要将table_structure里的所有字段都整合成K-V结构以构造sql，就像下面一样
                data_to_insert = {field: self.mysql_connection.escape(value[field]) if isinstance(value[field], str) else value[field] for
                                  field in table_structure}
                logging.debug(f"\033[34m====data_to_insert====\n{data_to_insert}\033[0m")

                columns = ', '.join(data_to_insert.keys())
                logging.debug(f"\033[34m====columns====\n{columns}\033[0m")

                values = ', '.join(data_to_insert.values())
                logging.debug(f"\033[34m====values====\n{values}\033[0m")

                sql = f"INSERT INTO {db_name} ({columns}) VALUES ({values})"

                # ------------------------执行SQL区------------------------
                logging.debug(f"\033[34m====SQL====\n{sql}\033[0m")

                redis_pipe = self.redis_connection.pipeline()  # 获取 Redis 客户端对象
                redis_pipe.multi()  # 开启事务
                self.mysql_connection.begin()  # 开启事务

                try:
                    self.mysql_cursor.execute(sql)
                    self.mysql_connection.commit()
                    logging.info("\033[32m====Mysql row committed====\033[0m")
                    redis_pipe.delete(key)
                    redis_result = redis_pipe.execute()
                    logging.info(f"\033[32m====Redis row deleted:{redis_result}====\033[0m")
                except Exception as e:
                    # 发生异常，回滚事务
                    self.mysql_connection.rollback()
                    # self.redis_pipe.execute()  # 自动回滚
                    logging.error(f"\033[31m====SQL Error Occurred: {e}====\033[0m")
                    raise e

    # redis key扫描模块
    def scan_keys(self, pattern, count):
        cursor = '0'
        keys = []
        while cursor != 0 and len(keys) < count:
            cursor, partial_keys = self.redis_connection.scan(cursor=cursor, match=pattern, count=count)
            keys.extend(partial_keys)
        return keys[:count]

    # 数据校验模块，传入value
    def data_validate(self, value):
        # 如果无answer或issue_desc，那就删除这部分
        for key, val in value.items():
            if val is None or val == '':
                return False
        return True

    # 数据去重模块，查询mysql，看看是否有一样的url，
    def is_url_duplicate(self, db_name, value, table_structure):
        # 定义可能的 URL 关键字列表
        url_keywords = ['url', 'href', 'link']

        # 在table_structure中查找包含 URL 关键字的元素，假设只有一个
        url_field_name = self.find_url_field_name(table_structure, url_keywords)

        if not url_field_name:
            return 0

        current_url = value.get(url_field_name, None)  # 获取当前行的url值
        # 在数据库中查询是否存在相同的 URL
        try:
            query = f"SELECT COUNT(*) FROM {db_name} WHERE {url_field_name} = %s"
            self.mysql_cursor.execute(query, (current_url,))
            count = self.mysql_cursor.fetchone()[0]
            return count
        except Exception as e:
            logging.warning(f"\033[33m====SQL Error: {e}====\033[0m")

    def find_url_field_name(self, table_structure, url_keywords):
        for keyword in url_keywords:
            for field in table_structure:
                if keyword in field.lower():
                    return field
        return None