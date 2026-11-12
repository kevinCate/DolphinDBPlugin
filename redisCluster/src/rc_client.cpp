//
// Created by uplee on 9/21/25.
//
#include "rc_client.h"
using namespace sw::redis;

RCClient& RCClient::instance() {
    static RCClient inst;
    return inst;
}

void RCClient::init(const Options& opt) {
    std::lock_guard<std::mutex> lk(mtx_);
    conn_ = {};
    conn_.host = opt.host;
    conn_.port = opt.port;
    if (!opt.password.empty()) conn_.password = opt.password;

    pool_ = {};
    pool_.size = opt.pool_size;

    cluster_.reset(new RedisCluster(conn_, pool_));
}

void RCClient::set(const std::string& key, const std::string& val) {
    std::lock_guard<std::mutex> lk(mtx_);
    cluster_->set(key, val);
}

std::string RCClient::get(const std::string& key) {
    std::lock_guard<std::mutex> lk(mtx_);
    auto v = cluster_->get(key);
    return v.value_or("");
}