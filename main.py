import logging
import time
import schedule

import Redis2Mysql

# MySQL 连接信息
mysql_host = '192.168.1.204'
mysql_port = 3308
mysql_user = 'root'
mysql_password = '123456'
mysql_database = 'spider01'

# Redis 连接信息
redis_host = '192.168.1.212'
redis_port = 6379
redis_db = 15

# 批处理数量
batch_size = 10

# 定义数据表名
db_name_list = ['scrapy_xywy', 'scrapy_haodf', 'scrapy_sprain_doc']

# 每个数据表的字段类型
table_structure_1 = ['title', 'quest', 'url', 'answer', 'doc_name', 'doc_posts', 'department',
                     'hos_grade', 'hos_name']
table_structure_2 = ['answer_url', 'disease', 'diseaseinfo', 'suggestions', 'grade', 'status',
                     'faculty', 'faculty_href']
table_structure_3 = ['issue_title', 'issue_desc', 'case_url', 'answer', 'already_parsed']

SYSTEM_EXEC_TURNS = 0

logging.basicConfig(level=logging.DEBUG)


def schedule_job():
    global SYSTEM_EXEC_TURNS
    print("====Executing scheduled job: sync_redis_2_mysql====")

    redis2mysql_instance = Redis2Mysql.Redis2Mysql(mysql_host,
                                                   mysql_port,
                                                   mysql_user,
                                                   mysql_password,
                                                   mysql_database,
                                                   redis_host,
                                                   redis_port,
                                                   redis_db,
                                                   batch_size,
                                                   db_name_list,
                                                   table_structure_1,
                                                   table_structure_2,
                                                   table_structure_3)  # TODO 在这里传入可变数量参数

    redis2mysql_instance.sync_redis_2_mysql()
    SYSTEM_EXEC_TURNS += 1
    logging.info(f"\033[32m========Scheduled job: sync_redis_2_mysql Finished========\033[0m")
    logging.info(f"\033[32m========SYSTEM_EXEC_TURNS: {SYSTEM_EXEC_TURNS}========\033[0m")


# schedule.every().day.at("20:00").do(schedule_job)
# schedule.every(5).minutes.do(schedule_job)
# schedule.every(24).hour.at("20:00").do(schedule_job)  # 每天晚上8点触发一次+
# schedule.every(24).hour.at("08:00").do(schedule_job)  # 每天晚上8点触发一次

schedule.every(10).seconds.do(schedule_job)

if __name__ == '__main__':
    print("====Welcome to Database Sync====")
    while True:
        schedule.run_pending()
        time.sleep(5)  # 避免不必要的消耗
