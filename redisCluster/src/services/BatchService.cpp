#include "services/BatchService.h"
#include <cstring>
#include <sw/redis++/redis++.h>
#include "core/Router.h"
#include "core/RedisTaskDispatcher.h"
#include "core/Utils.h"

using sw::redis::OptionalString;
using sw::redis::StringView;
using rc::BatchService;
using rc::utils::ds_len;
using rc::utils::ds_ptr;

namespace rc {

void BatchService::batchGet(const std::vector<std::string>& keys,
                       std::vector<OptionalString>& out) const
{
    // Note: we ignore threading for mget in this option, but grouping is reused
    out.clear();
    out.resize(keys.size());
    if (keys.empty()) return;

    auto& rcx = conn_.rc();

    // Build tasks grouping by tag / slot
    auto tasks = group_by_single_slot(keys);
    auto spKeys = std::make_shared<std::vector<std::string>>(keys);

    // For each task, fetch all keys in that group via mget
    for (const auto& task : tasks) {
        // Build vector of key strings for this group
        std::vector<std::string> ks;
        ks.reserve(task.rows.size());
        for (auto idx : task.rows) {
            ks.push_back((*spKeys)[idx]);
        }
        // Perform mget in one shot
        std::vector<OptionalString> tmp;
        tmp.reserve(ks.size());
        rcx.mget(ks.begin(), ks.end(), std::back_inserter(tmp));
        // Copy results back into `out`
        for (size_t j = 0; j < tmp.size(); ++j) {
            out[task.rows[j]] = std::move(tmp[j]);
        }
    }
}

void BatchService::batchSet(std::vector<std::string> keys, std::vector<std::string> values, int numThreads) const
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


void BatchService::batchHSet(const std::vector<std::string>& keys,
    const ddb::TableSP& fieldData,
    int numThreads) const {

    const std::size_t N = fieldData->size();
    if (N == 0) return;
    if (keys.size() != N)
        throw ddb::IllegalArgumentException(__FUNCTION__, "keys and fieldData must have same number of rows");

    const int numCols = fieldData->columns();
    if (numCols <= 0) return;

    // 1) Materialize shared field names and column data; keep owners alive
    auto spFieldNames = std::make_shared<std::vector<std::string>>(numCols);
    std::vector<ddb::VectorSP> cols(numCols);
    auto spColData = std::make_shared<std::vector<ddb::DolphinString*>>(numCols);

    for (int c = 0; c < numCols; ++c) {
        (*spFieldNames)[c] = fieldData->getColumnName(c);
        cols[c] = fieldData->getColumn(c);
        if (cols[c]->getType() != ddb::DT_STRING)
            throw ddb::RuntimeException("[Plugin::RedisCluster] fieldData columns must be STRING");
        (*spColData)[c] = static_cast<ddb::DolphinString*>(cols[c]->getDataArray());
        if (!(*spColData)[c])
            throw ddb::RuntimeException("[Plugin::RedisCluster] getDataArray() returned null");
    }

    // Optional: keep VectorSP owners alive if the dispatcher becomes async later
    auto spCols = std::make_shared<std::vector<ddb::VectorSP>>(cols.begin(), cols.end());

    // Pre-build StringView for field names; capture spFieldNames to keep storage alive
    auto spFieldNameViews = std::make_shared<std::vector<sw::redis::StringView>>(numCols);
    for (int c = 0; c < numCols; ++c) {
        (*spFieldNameViews)[c] = sw::redis::StringView((*spFieldNames)[c]);
    }

    // 2) Group rows by route and build command-centric tasks (one HSET per row)
    std::vector<CommandTask> cmdTasks;
    cmdTasks.reserve(N);

    auto groups = group_by_single_slot(keys);
    for (auto& g : groups) {
        for (auto row : g.rows) {
            const std::string& key = keys[row]; // copy to decouple from caller storage

            CommandTask t;
            t.routeKey = g.routeKey;

            // direct execution on RedisCluster
            t.execDirect = [spFieldNames, spFieldNameViews, spColData, spCols, numCols, key, row](sw::redis::RedisCluster& rcx){
                (void)spFieldNames; // keep field-name storage alive for StringView
                (void)spCols;       // keep VectorSP owners alive (future-proof)

                std::vector<std::pair<sw::redis::StringView, sw::redis::StringView>> fvs;
                fvs.reserve(static_cast<std::size_t>(numCols));
                for (int c = 0; c < numCols; ++c) {
                    const ddb::DolphinString& ds = (*spColData)[c][row];
                    fvs.emplace_back(
                        (*spFieldNameViews)[c],
                        sw::redis::StringView(ds_ptr(ds), ds_len(ds))
                    );
                }
                rcx.hset(key, fvs.begin(), fvs.end());
            };

            // pipelined: append a single HSET for this row
            t.execPiped = [spFieldNames, spFieldNameViews, spColData, spCols, numCols, key, row](sw::redis::Pipeline& pipe){
                (void)spFieldNames; // keep field-name storage alive for StringView
                (void)spCols;       // keep VectorSP owners alive (future-proof)
                std::vector<std::pair<sw::redis::StringView, sw::redis::StringView>> fvs;
                fvs.reserve(static_cast<std::size_t>(numCols));
                for (int c = 0; c < numCols; ++c) {
                    const ddb::DolphinString& ds = (*spColData)[c][row];
                    fvs.emplace_back(
                        (*spFieldNameViews)[c],
                        sw::redis::StringView(ds_ptr(ds), ds_len(ds))
                    );
                }
                pipe.hset(key, fvs.begin(), fvs.end());
            };

            cmdTasks.emplace_back(std::move(t));
        }
    }

    // 3) Dispatch with generic command scheduler
    dispatchCommandTasks(conn_, std::move(cmdTasks), numThreads);
}

void BatchService::batchDel(const std::vector<std::string>& keys,
                                 bool useUnlink,
                                 int numThreads) const
{
    if (keys.empty()) return;

    // Group by single slot or tag to ensure each pipeline binds to one route
    auto groups = group_by_single_slot(keys);

    // keys-per-command (distinct from pipeline window)
    const std::size_t opBatch = std::max<std::size_t>(1, conn_.policy().arg_batch);

    std::vector<CommandTask> tasks;
    tasks.reserve(groups.size());

    for (auto& g : groups) {
        // materialize group keys; kept alive by shared_ptr captured in lambdas
        auto spKeys = std::make_shared<std::vector<std::string>>();
        spKeys->reserve(g.rows.size());
        for (auto idx : g.rows) spKeys->emplace_back(keys[idx]);

        std::size_t p = 0, n = spKeys->size();
        while (p < n) {
            const std::size_t upto = std::min(n, p + opBatch);

            CommandTask t;
            t.routeKey = g.routeKey;

            // direct: one DEL/UNLINK call with a range of keys
            if (useUnlink) {
                t.execDirect = [spKeys, p, upto](sw::redis::RedisCluster& rcx){
                    rcx.unlink(spKeys->begin()+static_cast<std::ptrdiff_t>(p),
                               spKeys->begin()+static_cast<std::ptrdiff_t>(upto));
                };
                t.execPiped = [spKeys, p, upto](sw::redis::Pipeline& pipe){
                    // conservative: append per-key sub-commands to avoid assuming Pipeline has range overload
                    for (std::size_t i = p; i < upto; ++i) pipe.unlink((*spKeys)[i]);
                };
            } else {
                t.execDirect = [spKeys, p, upto](sw::redis::RedisCluster& rcx){
                    rcx.del(spKeys->begin()+static_cast<std::ptrdiff_t>(p),
                            spKeys->begin()+static_cast<std::ptrdiff_t>(upto));
                };
                t.execPiped = [spKeys, p, upto](sw::redis::Pipeline& pipe){
                    for (std::size_t i = p; i < upto; ++i) pipe.del((*spKeys)[i]);
                };
            }

            tasks.emplace_back(std::move(t));
            p = upto;
        }
    }

    dispatchCommandTasks(conn_, std::move(tasks), numThreads);
}

}