#/bin/bash

docker compose up -d

cp /tmp/tmp.VMdgjBNhq0/cmake-build-release/redisCluster/libPluginRedisCluster.so . 
cp /tmp/tmp.VMdgjBNhq0/redisCluster/PluginRedisCluster.txt .

docker exec plugin-ddb mkdir -p /data/ddb/server/plugins/redisCluster
docker cp /tmp/tmp.VMdgjBNhq0/cmake-build-release/redisCluster/libPluginRedisCluster.so  plugin-ddb:/data/ddb/server/plugins/redisCluster/
docker cp /tmp/tmp.VMdgjBNhq0/redisCluster/PluginRedisCluster.txt  plugin-ddb:/data/ddb/server/plugins/redisCluster/