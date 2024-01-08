import logging
import time
import schedule

from Redis2Mysql import sync_redis_2_mysql

# MySQL 连接信息
mysql_host = '192.168.1.204'
mysql_port = 3308
mysql_user = 'root'
mysql_password = '123456'
mysql_database = 'spider01'

# Redis 连接信息
redis_host = '192.168.1.212'
redis_port = 6379
redis_db = 0

logging.basicConfig(level=logging.DEBUG)


def schedule_job():
    print("====Executing scheduled job: sync_redis_2_mysql====")
    sync_redis_2_mysql(mysql_host, mysql_port, mysql_user, mysql_password,
                       mysql_database, redis_host, redis_port, redis_db)
    print("====Scheduled job: sync_redis_2_mysql Finished====")


# schedule.every().day.at("20:00").do(schedule_job)
# schedule.every(5).minutes.do(schedule_job)
schedule.every(10).seconds.do(schedule_job)

if __name__ == '__main__':
    while True:
        schedule.run_pending()
        time.sleep(5)  # 避免不必要的消耗
