//
// Created by uplee on 10/2/25.
//

#include "KVService.h"
#include "RedisTaskDispatcher.h"
#include "core/Router.h"

namespace rc {

sw::redis::OptionalString KVService::get(const std::string& key) const
{
    return conn_.rc().get(key);
}

void KVService::set(const std::string& key, const std::string& val) const
{
    conn_.rc().set(key, val);
}

void KVService::setex(const std::string& key, const std::string& val, int ttl_sec) const
{
    conn_.rc().setex(key, std::chrono::seconds(ttl_sec), val);
}

void KVService::batchSet(std::vector<std::string> keys, std::vector<std::string> values, int numThreads) const
{
    const std::size_t N = keys.size();
    if (N == 0) return;
    if (values.size() != N)
        throw ddb::RuntimeException("[Plugin::RedisCluster] keys and values must have same size");

    // route grouping to keep each pipeline bound to a single node
    auto groups = group_by_single_slot(keys);

    std::vector<CommandTask> tasks;
    tasks.reserve(N);

    // keep storage alive for lambdas
    auto spKeys = std::make_shared<std::vector<std::string>>(std::move(keys));
    auto spVals = std::make_shared<std::vector<std::string>>(std::move(values));

    for (auto& g : groups) {
        for (auto row : g.rows) {
            CommandTask t;
            t.routeKey = g.routeKey;

            t.execDirect = [spKeys, spVals, row](sw::redis::RedisCluster& rcx){
                rcx.set((*spKeys)[row], (*spVals)[row]);
            };
            t.execPiped = [spKeys, spVals, row](sw::redis::Pipeline& pipe){
                pipe.set((*spKeys)[row], (*spVals)[row]);
            };
            tasks.emplace_back(std::move(t));
        }
    }

    dispatchCommandTasks(conn_, std::move(tasks), numThreads);
}

}