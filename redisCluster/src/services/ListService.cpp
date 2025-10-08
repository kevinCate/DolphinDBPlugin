//
// Created by uplee on 10/8/25.
//

#include "services/ListService.h"
#include "core/Router.h"
#include "RedisTaskDispatcher.h"
#include <sw/redis++/redis++.h>
#include <cstring>

namespace {
inline const char* ds_ptr(const ddb::DolphinString& ds){ return ds.c_str(); }
inline std::size_t ds_len(const ddb::DolphinString& ds){ return std::strlen(ds.c_str()); }
}

namespace rc {

void ListService::batchPush(const std::vector<std::string>& keys,
    const std::vector<ddb::VectorSP>& vals,
    bool rightPush,
    int numThreads) const
    {
    if (keys.empty()) return;
    if (keys.size() != vals.size())
        throw ddb::IllegalArgumentException(__FUNCTION__, "[Plugin::RedisCluster] keys and values must have same length.");

    // Group by tag/slot
    auto groups = group_by_single_slot(keys);
    std::vector<CommandTask> tasks;
    tasks.reserve(keys.size());

    for (const auto& g : groups) {
        for (auto row : g.rows) {
            const std::string& key = keys[row];        // copy key text
            const ddb::VectorSP& valVec  = vals[row];        // keep inner vector alive

            CommandTask t;
            t.routeKey = g.routeKey;

            // Direct execution (single command with all elements).
            t.execDirect = [key, valVec, rightPush](sw::redis::RedisCluster& rcx){
                const auto n = valVec->size();
                std::vector<sw::redis::StringView> elems;
                elems.reserve(static_cast<std::size_t>(n));

                auto* arr = static_cast<ddb::DolphinString*>(valVec->getDataArray());
                if (!arr) throw ddb::RuntimeException("[Plugin::RedisCluster] invalid STRING vector storage.");

                for (int i = 0; i < n; ++i) elems.emplace_back(ds_ptr(arr[i]), ds_len(arr[i]));
                if (rightPush) rcx.rpush(key, elems.begin(), elems.end());
                else           rcx.lpush(key, elems.begin(), elems.end());
            };

            // Pipelined execution: append one RPUSH/LPUSH for this key.
            t.execPiped = [key, valVec, rightPush](sw::redis::Pipeline& pipe){
                const auto n = valVec->size();
                std::vector<sw::redis::StringView> elems;
                elems.reserve(static_cast<std::size_t>(n));

                auto* arr = static_cast<ddb::DolphinString*>(valVec->getDataArray());
                if (!arr) throw ddb::RuntimeException("[Plugin::RedisCluster] invalid STRING vector storage.");

                for (int i = 0; i < n; ++i) elems.emplace_back(ds_ptr(arr[i]), ds_len(arr[i]));
                if (rightPush) pipe.rpush(key, elems.begin(), elems.end());
                else           pipe.lpush(key, elems.begin(), elems.end());
            };

            tasks.emplace_back(std::move(t));
        }
    }

    // Dispatch with multi-thread/pipeline policy
    dispatchCommandTasks(conn_, std::move(tasks), numThreads);
}

} // namespace rc