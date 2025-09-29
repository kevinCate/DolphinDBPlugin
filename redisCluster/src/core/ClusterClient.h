//
// Created by uplee on 9/25/25.
//

#ifndef CLUSTERCLIENT_H
#define CLUSTERCLIENT_H

#pragma once
#include "rc_connection.h"
#include "DolphinDBEverything.h"

class ClusterClient {
public:
    explicit ClusterClient(RedisClusterConn& holder) : holder_(holder) {}

    // --- KV ---
    sw::redis::OptionalString get(const std::string& key) const;
    void set(const std::string& key, const std::string& val) const;
    void setex(const std::string& key, const std::string& val, int ttl_sec) const;

    void mget(const std::vector<std::string>& keys, std::vector<sw::redis::OptionalString>& out) const;

    // 预留：hash / list / pipeline ...
    // void hset(...);
    // void lpush(...);
    // void pipelineSet(...);

    // === 新增：批量 HSET（每行多字段），带窗口 & hash-tag 分组，面向 Cluster 优化 ===
    void batchHashSet(const std::vector<std::string>& keys,
                      const ddb::TableSP& fieldData,             // 所有列必须为 STRING
                      std::size_t batchWin = 2048) const;

private:
    RedisClusterConn& holder_;
};

#endif //CLUSTERCLIENT_H
