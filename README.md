# DatabaseSync

## 部署

### Docker部署
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