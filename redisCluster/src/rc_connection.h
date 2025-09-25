//
// Created by uplee on 9/23/25.
//

#ifndef REDISCLUSTERCONNECTION_H
#define REDISCLUSTERCONNECTION_H

#pragma once
#include <memory>
#include <string>
#include "sw/redis++/redis++.h"

class RedisClusterConn {
public:
    explicit RedisClusterConn(std::unique_ptr<sw::redis::RedisCluster> cluster,
                              std::string seed)
    : cluster_(std::move(cluster)), seed_(std::move(seed)) {}

    sw::redis::RedisCluster& get() { return *cluster_; }
    const std::string& seed() const { return seed_; }

private:
    std::unique_ptr<sw::redis::RedisCluster> cluster_;
    std::string seed_;
};

#endif //REDISCLUSTERCONNECTION_H
