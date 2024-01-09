import logging

import pymysql
import redis


def sync_redis_2_mysql(mysql_host: str, mysql_port: int, mysql_user: str, mysql_password: str, mysql_database: str,
                       redis_host: str, redis_port: int, redis_db: int):
    try:
        # 连接到 MySQL 数据库，反正xywy, haodf, sprain都是在一个数据库里
        mysql_connection = pymysql.connect(host=mysql_host, port=mysql_port, user=mysql_user, password=mysql_password,
                                           database=mysql_database)
        logging.info(f"\033[32m====mysql_results====\n{mysql_connection}\033[0m")
        mysql_cursor = mysql_connection.cursor()
        logging.debug(f"\033[34m====mysql_cursor====\n{mysql_cursor}\033[0m")
        # mysql_cursor.execute("SELECT * FROM scrapy_sprain_doc")
        # mysql_results = mysql_cursor.fetchall()
        # logging.info(f"\033[32m====mysql_results====\n{mysql_results}\033[0m")
        # for row in mysql_results:
        #     logging.info(f"\033[32m====QUERY MYSQL====\n{row}\033[0m")

        # 定义数据表名
        scrapy_xywy = 'scrapy_xywy'
        scrapy_sprain_doc = 'scrapy_sprain_doc'
        scrapy_haodf = 'scrapy_haodf'

        # 连接到 Redis
        redis_connection = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db)

        # TODO 执行 Redis 查询特定的数据，来进行逐行同步到mysql中
        redis_results = redis_connection.keys('*')  # 以示例方式获取所有键
        for key in redis_results:
            logging.info(f"\033[32m====QUERY KEY====\n{key}\033[0m")
            # 获取Redis键的前缀
            prefix = key.split(':')[0]
            if prefix == 'xywy':
                # 将Redis键同步到MySQL表`xywy`中
                transfer_xywy(scrapy_xywy)
            elif prefix == 'haodf':
                # 将Redis键同步到MySQL表`haodf`中
                transfer_xywy(scrapy_sprain_doc)
            elif prefix == 'sprain':
                # 将Redis键同步到MySQL表`sprain`中
                transfer_xywy(scrapy_haodf)

    except pymysql.Error as mysql_error:
        logging.info(f"\033[33m====Mysql Error====\n{mysql_error}\033[0m")

    except redis.RedisError as redis_error:
        logging.info(f"\033[33m====Redis Error====\n{redis_error}\033[0m")

    finally:
        # 关闭 MySQL 连接和游标
        if 'mysql_cursor' in locals() and mysql_cursor is not None:
            mysql_cursor.close()
        if 'mysql_connection' in locals() and mysql_connection is not None:
            mysql_connection.close()

        # 关闭 Redis 连接
        if 'redis_connection' in locals() and redis_connection is not None:
            redis_connection.close()


def transfer_xywy(db_name: str):
    pass


def transfer_haodf(db_name: str):
    pass


def transfer_sprain(db_name: str):
    pass
