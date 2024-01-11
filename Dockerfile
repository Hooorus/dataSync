# 使用一个基础的Python镜像
FROM python:3.12

# 设置工作目录
WORKDIR /app

# 复制脚本和配置文件到容器内
COPY main.py .
COPY Redis2Mysql.py .
COPY config.yaml .

# 安装任何依赖，如果有的话
RUN pip install pyyaml pymysql redis

# 复制cron任务到/etc/cron.d/目录
COPY crontab /etc/cron.d/database_sync_cronjob

# 给crontab配置文件以及脚本执行权限
RUN chmod 0644 /etc/cron.d/my-cron && \
    chmod +x main.py && \
    chmod +x Redis2Mysql.py

# 创建cron日志文件
RUN touch /var/log/database_sync_cronjob.log

# 启动cron服务，tail -f以防止没有其他任务而退出
CMD cron && tail -f /var/log/cron.log