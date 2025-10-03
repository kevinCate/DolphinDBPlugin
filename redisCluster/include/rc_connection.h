//
// Created by uplee on 9/23/25.
//

#ifndef REDISCLUSTERCONNECTION_H
#define REDISCLUSTERCONNECTION_H

#pragma once
#include <memory>
#include "sw/redis++/redis++.h"

namespace rc {

class RedisClusterConn {
public:
    explicit RedisClusterConn(std::unique_ptr<sw::redis::RedisCluster> cluster)
    : cluster_(std::move(cluster)) {}

    [[nodiscard]] sw::redis::RedisCluster& get() const { return *cluster_; }

private:
    std::unique_ptr<sw::redis::RedisCluster> cluster_;
};

}
#endif //REDISCLUSTERCONNECTION_H
