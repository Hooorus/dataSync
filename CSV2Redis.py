import uuid

import csv
import logging
import re

import redis


def read_from_file(target_list_container: list, file_path: str, enable_regex: int):
    logging.info("\033[32m=====Entering read_from_file=====\n\033[0m")
    try:
        with open(f"{file_path}", "r+", newline='', encoding='utf-8') as csvfile:
            logging.info(f"\033[34m=====Opened {file_path}=====\n\033[0m")
            reader = csv.DictReader(csvfile)
            rows_to_write = []  # 用于存储将要写回文件的行

            if enable_regex == 1:
                url_pattern = re.compile(r'https?://\S+')  # url正则匹配
            elif enable_regex == 0:
                url_pattern = re.compile(r'^(?!URL).*')
            for row in reader:  # 读取每行
                logging.debug(f"\033[34m====={row}=====\n\033[0m")
                if url_pattern.match(row['URL']) and row['AlREADY_PARSED'] == "0":  # 如果匹配的话
                    row['AlREADY_PARSED'] = "1"
                    target_list_container.append(row['URL'])  # 装到一个list中
                    logging.debug(f"\033[34m=====Regex one row & appended to {target_list_container}=====\n\033[0m")
                # 将修改后的行添加到列表中
                rows_to_write.append(row)
            # 将修改后的行写回文件
            with open(f"{file_path}", 'w', newline='', encoding='utf-8') as csvfile_write:
                writer = csv.DictWriter(csvfile_write, fieldnames=reader.fieldnames)
                writer.writeheader()
                writer.writerows(rows_to_write)

            logging.info(
                f"\033[32m=====Finished loading {file_path} to {target_list_container}=====\n\033[0m")
            return target_list_container
    except Exception as e:
        logging.error(
            f"\033[31m=====Error in read_from_file occurred when reading {file_path}: {e}=====\n\033[0m")
        return target_list_container


class CSV2Redis:
    def __init__(self,
                 redis_host: str,
                 redis_port: int,
                 redis_db: int,
                 csv_file_path: str):

        self.csv_file_path = csv_file_path
        self.issue_url_list = []  # issue url装载容器

        try:
            # ------------初始化Redis------------
            self.redis_connection = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db)
            logging.info(f"\033[32m====Redis Connection Info====\n{self.redis_connection}\033[0m")
        except Exception as redis_error:
            logging.error(f"\033[31m====Failed to initialize Redis connection: {redis_error}\033[0m")

    def sync_csv_2_redis(self):
        read_from_file(self.issue_url_list, self.csv_file_path, 1)
        logging.info(f"\033[32m====len of read_from_file: {len(self.issue_url_list)}====\n")
        logging.info(f"\033[32m====Entering sync_csv_2_redis====\n")
        try:
            pipeline = self.redis_connection.pipeline()
            for url in self.issue_url_list:
                key = 'sprain:' + str(uuid.uuid4())  # Generating a new UUID for each URL
                pipeline.set(key, url)
                logging.info(f"\033[32m====Stored data in Redis:====\nKEY: {key} <=> URL: {url}\n")
            pipeline.execute()
            logging.info(f"\033[32m====Sync Finished====\n")
        except Exception as e:
            print(f"====Error storing data in Redis: {e}")
