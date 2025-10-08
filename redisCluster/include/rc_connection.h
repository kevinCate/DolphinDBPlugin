//
// Created by uplee on 9/23/25.
//

#ifndef REDISCLUSTERCONNECTION_H
#define REDISCLUSTERCONNECTION_H

#pragma once
#include <memory>
#include "sw/redis++/redis++.h"
#include "ddbplugin/Plugin.h"

namespace rc {

class RedisClusterConn {
public:
    explicit RedisClusterConn(std::unique_ptr<sw::redis::RedisCluster> cluster, std::string address)
    : cluster_(std::move(cluster)), address_(std::move(address))
    {
        std::time_t t = std::time(nullptr);
        std::tm* now = std::localtime(&t);
        if (now) {
            createdTime_ = ddb::DateTime(1900 + now->tm_year, 1 + now->tm_mon, now->tm_mday,
            now->tm_hour, now->tm_min, now->tm_sec);
        }
    }

    [[nodiscard]] sw::redis::RedisCluster& get() const { return *cluster_; }
    [[nodiscard]] const std::string& getAddress() const { return address_; }
    [[nodiscard]] ddb::DateTime getCreatedTime() const { return createdTime_; }

private:
    std::unique_ptr<sw::redis::RedisCluster> cluster_;
    std::string   address_;
    ddb::DateTime createdTime_;
};

}
#endif //REDISCLUSTERCONNECTION_H
