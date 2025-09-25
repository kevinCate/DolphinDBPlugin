//
// Created by uplee on 9/25/25.
//

#ifndef CLUSTERCLIENT_H
#define CLUSTERCLIENT_H

#pragma once
#include "rc_connection.h"

class ClusterClient {
public:
    explicit ClusterClient(RedisClusterConn& holder) : holder_(holder) {}

    // --- KV ---
    sw::redis::OptionalString get(const std::string& key);
    void set(const std::string& key, const std::string& val);
    void setex(const std::string& key, const std::string& val, int ttl_sec);

    // 简单 MGET（逐个 get；后续可用 hash tag + pipeline 优化）
    void mget(const std::vector<std::string>& keys,
              std::vector<sw::redis::OptionalString>& out);

    // 预留：hash / list / pipeline ...
    // void hset(...);
    // void lpush(...);
    // void pipelineSet(...);

private:
    RedisClusterConn& holder_;
};

#endif //CLUSTERCLIENT_H
