# DatabaseSync

## 功能
### CSV存入Redis
1. 将待发送的csv文件放入`/csv`文件夹下，然后在`config.yaml`里面写入csv文件的绝对路径
2. csv格式为：
   ```text
   URL               ALREADY_PARSED
   待存入的URL       是否处理，0：未处理；1：已处理
   ```
3. 设置好csv后上传到服务器
4. 在main.py里面激活
5. 按照部署模式进行部署即可

### Redis存入Mysql
1. 编辑`config.yaml`，设置需要存入mysql的表结构
2. 

## 部署模式

### 1. Docker部署
在本目录下构建docker镜像
```shell
docker build -t <this_project_name> .
```
构建完了push到docker hub
```shell
docker push <your_docker_hub_username>/<this_project_name>
```
在服务器上pull此镜像
```shell
docker pull <your_docker_hub_username>/<this_project_name>
```
运行 Docker 容器，-d 选项将容器放到后台运行
```shell
docker run -d <your-docker-hub-username>/<this_project_name>
```
监控日志和输出
```shell
docker logs <container_id>
```

### 2. 直接部署
前置条件：在linux服务器上已经安装好了conda，并且已经安装好了如下的包
```shell
conda install pyyaml pymysql
```
```shell
pip install redis
```
激活虚拟环境
```shell
conda activate <your_environment_name>
```
设置crontab
```shell
# 设置cron任务，每天13点执行脚本，<>替换成你自己的位置
# 0 18 * * * /home/lym/miniconda3/envs/database_sync/bin/python /home/lym/databaseSync/main.py > /home/lym/databaseSync/crontab.log 2>&1
0 18 * * * <path_to_conda_env_bin_python> <path_to_script_main_entry> > <your_log_path_and_name> 2>&1
```
