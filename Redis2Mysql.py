import logging

import pymysql
import redis


class Redis2Mysql:
    def __init__(self, mysql_host: str, mysql_port: int, mysql_user: str, mysql_password: str, mysql_database: str,
                 redis_host: str, redis_port: int, redis_db: int):
        # ------------初始化Redis------------
        self.redis_connection = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db)
        logging.info(f"\033[32m====self.redis_connection====\n{self.redis_connection}\033[0m")
        # ------------初始化Mysql------------
        self.mysql_connection = pymysql.connect(host=mysql_host, port=mysql_port, user=mysql_user,
                                                password=mysql_password,
                                                database=mysql_database)
        logging.info(f"\033[32m====self.mysql_connection====\n{self.mysql_connection}\033[0m")
        self.mysql_cursor = self.mysql_connection.cursor()
        logging.debug(f"\033[34m====self.mysql_cursor====\n{self.mysql_cursor}\033[0m")

    def sync_redis_2_mysql(self):
        try:
            # 定义数据表名
            scrapy_xywy = 'scrapy_xywy'
            scrapy_sprain_doc = 'scrapy_sprain_doc'
            scrapy_haodf = 'scrapy_haodf'
            redis_results = self.redis_connection.keys('*')  # 以示例方式获取所有键
            # while True:
            #     redis_results = redis_connection.scan(start_id, count=200)
            # redis_results = redis_results[:2]

            for key in redis_results:
                key = key.decode('utf-8')
                logging.info(f"\033[32m====QUERY KEY====\n{key}\033[0m")
                # 获取Redis键的前缀
                prefix = key.split(':')[0]
                if prefix == 'xywy':
                    # 将Redis键同步到MySQL表`xywy`中
                    self.transfer_xywy(key, scrapy_xywy)
                elif prefix == 'haodf':
                    # 将Redis键同步到MySQL表`haodf`中
                    self.transfer_haodf(key, scrapy_haodf)
                elif prefix == 'sprain':
                    # 将Redis键同步到MySQL表`sprain`中
                    self.transfer_sprain(key, scrapy_sprain_doc)

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

    # ----------------寻医问药网站的同步器----------------
    def transfer_xywy(self, key: str, db_name: str):
        value = self.redis_connection.hgetall(key)

        # --------解码所有键值对--------
        value = {k.decode('utf-8'): v.decode('utf-8') for k, v in value.items()}
        logging.info(f"\033[32m====redis value====\n{value}\033[0m")

        title = self.mysql_connection.escape(value['title'])
        quest = self.mysql_connection.escape(value['quest'])
        url = self.mysql_connection.escape(value['url'])
        answer = self.mysql_connection.escape(value['answer'])
        doc_name = self.mysql_connection.escape(value['doc_name'])
        doc_posts = self.mysql_connection.escape(value['doc_posts'])
        department = self.mysql_connection.escape(value['department'])
        hos_grade = self.mysql_connection.escape(value['hos_grade'])
        hos_name = self.mysql_connection.escape(value['hos_name'])

    # ----------------好大夫网站的同步器----------------
    def transfer_haodf(self, key: str, db_name: str):
        value = self.redis_connection.hgetall(key)

    # ----------------春雨医生网站的同步器----------------
    def transfer_sprain(self, key: str, db_name: str):
        value = self.redis_connection.hgetall(key)

        # --------解码所有键值对--------
        value = {k.decode('utf-8'): v.decode('utf-8') for k, v in value.items()}
        logging.info(f"\033[32m====redis value====\n{value}\033[0m")
        
        issue_title = self.mysql_connection.escape(value['issue_title'])
        issue_desc = self.mysql_connection.escape(value['issue_desc'])
        case_url = self.mysql_connection.escape(value['case_url'])
        answer = self.mysql_connection.escape(value['answer'])
        already_parsed = self.mysql_connection.escape(value['already_parsed'])

        # -------处理字符串-------这里不在VALUE里面加'的原因是escape已经帮我们加上去了
        sql = f"""
            INSERT INTO {db_name} (issue_title, issue_desc, answer, case_url, already_parsed)
            VALUES ({issue_title}, {issue_desc}, {answer}, {case_url}, {already_parsed})
            """
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
