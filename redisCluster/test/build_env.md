# 环境搭建

### 搭建 Redis Cluster
- 3 主 3 从架构
- 使用Docker Compose搭建
- 不设置密码

1. 启动 Redis Cluster 容器

```
yml=path/to/docker-compose.yml
dir=$( dirname $yml )
cd $dir
docker compose up -d --force-recreate redis1 redis2 redis3 redis4 redis5 redis6 redis-cluster-init redisinsight
```

2. 启动 DolphinDB 容器

```
docker compose up -d --force-recreate ddb
```

3. 安装redisCluster插件

```
/data/ddb/server/plugins
docker exec -it plugin-ddb mkdir /data/ddb/server/plugins/redisCluster/
docker cp libPluginRedisCluster.so plugin-ddb:/data/ddb/server/plugins/redisCluster/
docker cp PluginRedisCluster.txt plugin-ddb:/data/ddb/server/plugins/redisCluster/
```

4. 准备测试脚本

```
testDos=/path/to/test_redisCluster.dos
docker cp $testDos plugin-ddb:/root/
```

5. 进入 DolphinDB, 运行回归示例

```
loadPlugin("redisCluster")
go
test("/root/test_redisCluster.dos","/root/test_redisCluster_output.txt");
```