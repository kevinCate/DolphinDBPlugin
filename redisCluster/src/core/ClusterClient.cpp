//
// Created by uplee on 9/25/25.
//

#include "core/ClusterClient.h"
#include <sw/redis++/redis++.h>

using sw::redis::RedisCluster;

 sw::redis::OptionalString ClusterClient::get(const std::string& key){
    return holder_.get().get(key);
}

void ClusterClient::set(const std::string& key, const std::string& val){
    holder_.get().set(key, val);
}

void ClusterClient::setex(const std::string& key, const std::string& val, int ttl_sec){
    holder_.get().setex(key, std::chrono::seconds(ttl_sec), val);
}

void ClusterClient::mget(const std::vector<std::string>& keys,
                         std::vector<sw::redis::OptionalString>& out){
    out.resize(keys.size());
    // 先朴素循环；将来可以按 slot 分组 + pipeline 优化（Cluster下MGET并非原子）。
    for (size_t i=0;i<keys.size();++i)
        out[i] = holder_.get().get(keys[i]);
}