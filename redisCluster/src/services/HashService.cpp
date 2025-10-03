#include "HashService.h"
#include <sw/redis++/redis++.h>
#include <unordered_map>
#include <thread>
#include <cstring>
#include "Router.h"

using sw::redis::OptionalString;
using sw::redis::StringView;
using rc::HashService;

namespace {

const char* ds_ptr(const ddb::DolphinString& ds) { return ds.c_str(); }
std::size_t ds_len(const ddb::DolphinString& ds) { return std::strlen(ds.c_str()); }

struct SlotBucket { std::vector<std::size_t> rows; };
struct TagBucket  { std::vector<std::size_t> rows; };

class RCSetWorker : public ddb::Runnable {
public:
    RCSetWorker(
        rc::ConnFacade& conn,
        std::shared_ptr<const std::vector<std::string>> keys,
        std::shared_ptr<const std::vector<std::string>> fieldNames,
        std::shared_ptr<const std::vector<ddb::DolphinString*>> colData,
        int numCols,
        std::string routeKey,
        std::vector<std::size_t> rows,
        std::size_t minPipe)
    : conn_(conn),
    keys_(std::move(keys)),
    fieldNames_(std::move(fieldNames)),
    colData_(std::move(colData)),
    numCols_(numCols),
    routeKey_(std::move(routeKey)),
    rows_(std::move(rows)),
    minPipe_(minPipe) {}

    void run() override {
        auto& rcx = conn_.rc();

        if (rows_.size() < 6) { // small path: direct
            std::vector<std::pair<StringView, StringView>> fvs; fvs.reserve(numCols_);
            for (auto r : rows_) {
                fvs.clear();
                for (int c = 0; c < numCols_; ++c) {
                    const auto& ds = (*colData_)[c][r];
                    fvs.emplace_back(StringView((*fieldNames_)[c]),
                                     StringView(ds_ptr(ds), ds_len(ds)));
                }
                rcx.hset((*keys_)[r], fvs.begin(), fvs.end());
            }
            return;
        }

        // Force a new connection per worker regardless of global policy.
        std::size_t p = 0;
        while (p < rows_.size()) {
            const std::size_t upto = std::min(rows_.size(), p + std::max<std::size_t>(2048, 6));
            auto pipe = rcx.pipeline(routeKey_, /*new_connection=*/true);
            std::vector<std::pair<StringView, StringView>> fvs; fvs.reserve(numCols_);
            for (; p < upto; ++p) {
                const auto r = rows_[p];
                fvs.clear();
                for (int c = 0; c < numCols_; ++c) {
                    const auto& ds = (*colData_)[c][r];
                    fvs.emplace_back(StringView((*fieldNames_)[c]),
                                     StringView(ds_ptr(ds), ds_len(ds)));
                }
                pipe.hset((*keys_)[r], fvs.begin(), fvs.end());
            }
            pipe.exec();
        }
    }
private:
    rc::ConnFacade& conn_;
    std::shared_ptr<const std::vector<std::string>> keys_;
    std::shared_ptr<const std::vector<std::string>> fieldNames_;
    std::shared_ptr<const std::vector<ddb::DolphinString*>> colData_;
    int numCols_;
    std::string routeKey_;
    std::vector<std::size_t> rows_;
    std::size_t minPipe_;
};

}

namespace rc {

// TODO: 是否需要添加pipeline
void HashService::mget(const std::vector<std::string>& keys, std::vector<OptionalString>& out) const {
    out.clear();
    out.resize(keys.size());
    if (keys.empty()) return;

    // Fast path: all keys share the same non-empty hashtag.
    const std::string_view tag0 = rc::extract_tag(keys[0]);
    bool all_same_tag = !tag0.empty();
    for (std::size_t i = 1; i < keys.size() && all_same_tag; ++i)
        if (rc::extract_tag(keys[i]) != tag0) all_same_tag = false;

    auto& rcx = conn_.rc();
    if (all_same_tag) {
        std::vector<OptionalString> tmp;
        tmp.reserve(keys.size());
        rcx.mget(keys.begin(), keys.end(), std::back_inserter(tmp));
        for (std::size_t i = 0; i < tmp.size(); ++i) out[i] = std::move(tmp[i]);
        return;
    }

    // General case: group by tag first; untagged -> group by slot; then mget per group.
    std::unordered_map<std::string_view, TagBucket> by_tag;
    std::vector<std::size_t> no_tag;
    by_tag.reserve(keys.size()); no_tag.reserve(keys.size());

    for (std::size_t i = 0; i < keys.size(); ++i) {
        std::string_view t = rc::extract_tag(keys[i]);
        if (t.empty()) no_tag.push_back(i);
        else           by_tag[t].rows.push_back(i);
    }

    // Tagged groups
    for (auto& kv : by_tag) {
        auto& rows = kv.second.rows;
        if (rows.empty()) continue;
        std::vector<std::string> k; k.reserve(rows.size());
        for (auto r : rows) k.emplace_back(keys[r]);
        std::vector<OptionalString> tmp; tmp.reserve(k.size());
        rcx.mget(k.begin(), k.end(), std::back_inserter(tmp));
        for (std::size_t j = 0; j < rows.size(); ++j) out[rows[j]] = std::move(tmp[j]);
    }

    // Untagged groups: compute slot and mget per slot bucket
    std::unordered_map<int, SlotBucket> by_slot;
    by_slot.reserve(no_tag.size());
    for (auto i : no_tag) {
        const int slot = rc::key_slot(std::string_view{keys[i]});
        by_slot[slot].rows.push_back(i);
    }
    for (auto& kv : by_slot) {
        auto& rows = kv.second.rows;
        if (rows.empty()) continue;
        std::vector<std::string> k; k.reserve(rows.size());
        for (auto r : rows) k.emplace_back(keys[r]);
        std::vector<OptionalString> tmp; tmp.reserve(k.size());
        rcx.mget(k.begin(), k.end(), std::back_inserter(tmp));
        for (std::size_t j = 0; j < rows.size(); ++j) out[rows[j]] = std::move(tmp[j]);
    }
}

void HashService::batchHSet(const std::vector<std::string>& keys, const ddb::TableSP& fieldData) const {
    const auto N = static_cast<std::size_t>(fieldData->size());
    if (N == 0) return;
    if (keys.size() != N)
        throw ddb::IllegalArgumentException(__FUNCTION__, "keys and fieldData must have same number of rows");

    const int numCols = fieldData->columns();
    if (numCols <= 0) return;

    // Prepare field names and STRING column data.
    std::vector<std::string> fieldNames(numCols);
    std::vector<ddb::VectorSP> cols(numCols);
    std::vector<ddb::DolphinString*> colData(numCols, nullptr);
    for (int c = 0; c < numCols; ++c) {
        fieldNames[c] = fieldData->getColumnName(c);
        cols[c] = fieldData->getColumn(c);
        if (cols[c]->getType() != ddb::DT_STRING)
            throw ddb::RuntimeException("[Plugin::RedisCluster] fieldData columns must be STRING");
        colData[c] = static_cast<ddb::DolphinString*>(cols[c]->getDataArray());
        if (!colData[c]) throw ddb::RuntimeException("[Plugin::RedisCluster] getDataArray() returned null");
    }
    std::vector<StringView> fnames(numCols);
    for (int c = 0; c < numCols; ++c) fnames[c] = StringView(fieldNames[c]);

    // Group by tag first.
    std::unordered_map<std::string_view, TagBucket> by_tag;
    std::vector<std::size_t> no_tag;
    by_tag.reserve(N); no_tag.reserve(N);
    for (std::size_t i = 0; i < N; ++i) {
        auto t = rc::extract_tag(keys[i]);
        if (t.empty()) no_tag.push_back(i);
        else           by_tag[t].rows.push_back(i);
    }

    // Tagged groups -> single-slot pipeline via ConnFacade helper.
    for (auto& kv : by_tag) {
        auto& rows = kv.second.rows;
        if (rows.empty()) continue;

        // One lambda builds a single HSET; ConnFacade chooses direct vs pipeline and windows it.
        conn_.pipeline_windowed(std::string(kv.first), rows.size(), // TODO: 看是否要改变pipeline_windowed参数为string_view
            [&](sw::redis::Pipeline* pipe, std::size_t i) {
                const std::size_t row = rows[i];
                std::vector<std::pair<StringView, StringView>> fvs;
                fvs.reserve(numCols);
                for (int c = 0; c < numCols; ++c) {
                    const auto& ds = colData[c][row];
                    fvs.emplace_back(fnames[c], StringView(ds_ptr(ds), ds_len(ds)));
                }
                if (!pipe) {
                    conn_.rc().hset(keys[row], fvs.begin(), fvs.end());
                } else {
                    pipe->hset(keys[row], fvs.begin(), fvs.end());
                }
            });
    }

    // Untagged -> group by slot first, then issue via helper.
    std::unordered_map<int, SlotBucket> by_slot;
    by_slot.reserve(no_tag.size());
    for (auto i : no_tag) {
        const int slot = rc::key_slot(std::string_view{keys[i]});
        by_slot[slot].rows.push_back(i);
    }

    for (auto& kv : by_slot) {
        auto& rows = kv.second.rows;
        if (rows.empty()) continue;

        const std::string& routeKey = keys[rows.front()]; // representative to bind slot
        conn_.pipeline_windowed(routeKey, rows.size(),
            [&](sw::redis::Pipeline* pipe, std::size_t i) {
                const std::size_t row = rows[i];
                std::vector<std::pair<StringView, StringView>> fvs;
                fvs.reserve(numCols);
                for (int c = 0; c < numCols; ++c) {
                    const auto& ds = colData[c][row];
                    fvs.emplace_back(fnames[c], StringView(ds_ptr(ds), ds_len(ds)));
                }
                if (!pipe) {
                    conn_.rc().hset(keys[row], fvs.begin(), fvs.end());
                } else {
                    pipe->hset(keys[row], fvs.begin(), fvs.end());
                }
            });
    }
}

void HashService::batchHSetThread(const std::vector<std::string>& keys, const ddb::TableSP& fieldData, int numThreads) const {
    const auto N = static_cast<std::size_t>(fieldData->size());
    if (N == 0) return;
    if (keys.size() != N)
        throw ddb::IllegalArgumentException(__FUNCTION__, "keys and fieldData must have same number of rows");
    const int numCols = fieldData->columns();
    if (numCols <= 0) return;

    auto fieldNames = std::make_shared<std::vector<std::string>>(numCols);
    std::vector<ddb::VectorSP> cols(numCols);
    auto colData = std::make_shared<std::vector<ddb::DolphinString*>>(numCols, nullptr);
    for (int c = 0; c < numCols; ++c) {
        (*fieldNames)[c] = fieldData->getColumnName(c);
        cols[c] = fieldData->getColumn(c);
        if (cols[c]->getType() != ddb::DT_STRING)
            throw ddb::RuntimeException("[Plugin::RedisCluster] fieldData columns must be STRING");
        (*colData)[c] = static_cast<ddb::DolphinString*>(cols[c]->getDataArray());
        if (!(*colData)[c]) throw ddb::RuntimeException("[Plugin::RedisCluster] getDataArray() returned null");
    }

    // Groups: tag -> rows; untagged -> slot -> rows
    std::unordered_map<std::string_view, TagBucket> tagged;
    std::vector<std::size_t> no_tag;
    tagged.reserve(N); no_tag.reserve(N);
    for (std::size_t i = 0; i < N; ++i) {
        auto t = rc::extract_tag(keys[i]);
        if (t.empty()) no_tag.push_back(i);
        else           tagged[t].rows.push_back(i);
    }

    std::vector<std::pair<std::string, std::vector<std::size_t>>> tasks;
    tasks.reserve(tagged.size());
    for (auto& kv : tagged) {
        if (!kv.second.rows.empty())
            tasks.emplace_back(kv.first, std::move(kv.second.rows));
    }
    std::unordered_map<int, SlotBucket> by_slot;
    by_slot.reserve(no_tag.size());
    for (auto i : no_tag) {
        const int slot = rc::key_slot(std::string_view{keys[i]});
        by_slot[slot].rows.push_back(i);
    }
    for (auto& kv : by_slot) {
        if (!kv.second.rows.empty())
            tasks.emplace_back(keys[kv.second.rows.front()], std::move(kv.second.rows));
    }

    const unsigned hw = std::max(1u, std::thread::hardware_concurrency());
    int desired = (numThreads <= 0 ? 3 : numThreads);
    int threads = std::max(1, std::min<int>(desired, static_cast<int>(hw)));
    if (!tasks.empty()) threads = std::min<int>(threads, static_cast<int>(tasks.size()));

    auto spKeys = std::make_shared<const std::vector<std::string>>(keys);
    auto spNames= std::make_shared<const std::vector<std::string>>(*fieldNames);

    std::vector<ddb::ThreadSP> ths;
    ths.reserve(tasks.size());

    // Round-robin shard
    std::vector<std::vector<std::pair<std::string, std::vector<std::size_t>>>> shards(threads);
    for (std::size_t i = 0; i < tasks.size(); ++i)
        shards[i % threads].emplace_back(std::move(tasks[i]));

    for (int i = 0; i < threads; ++i) {
        for (auto& t : shards[i]) {
            auto* w = new RCSetWorker(conn_, spKeys, spNames, colData, numCols,
                                      std::move(t.first), std::move(t.second),
                                      conn_.policy().min_pipeline);
            ddb::ThreadSP thr = new ddb::Thread(w);
            if (!thr->isStarted()) thr->start();
            ths.emplace_back(std::move(thr));
        }
    }
    for (auto& th : ths) th->join();
}

void HashService::deleteKeys(const std::vector<std::string>& keys, bool useUnlink) const {
    if (keys.empty()) return;

    auto& rcx = conn_.rc();
    const auto& pol = conn_.policy(); // snapshot knobs

    const std::string_view tag0 = rc::extract_tag(keys[0]);
    bool all_same_tag = !tag0.empty();
    for (std::size_t i = 1; i < keys.size() && all_same_tag; ++i)
        if (rc::extract_tag(keys[i]) != tag0) all_same_tag = false;

    auto do_window_cmd = [&](auto first, auto last){
        if (useUnlink) rcx.unlink(first, last);
        else           rcx.del(first, last);
    };

    if (all_same_tag) {
        const std::size_t n = keys.size();
        if (n < pol.min_pipeline) {
            // Keep multi-key window optimization on the direct path.
            for (std::size_t p = 0; p < n; p += pol.batch_window) {
                const std::size_t upto = std::min(n, p + pol.batch_window);
                // 显式把 size_t 转成 difference_type
                auto first = std::next(keys.begin(), static_cast<std::vector<std::string>::difference_type>(p));
                auto last  = std::next(keys.begin(), static_cast<std::vector<std::string>::difference_type>(upto));
                do_window_cmd(first, last);
            }
        } else {
            // Use ConnFacade helper to pipeline single-key deletions per window.
            conn_.pipeline_windowed(std::string(tag0), n, // TODO
                [&](sw::redis::Pipeline* pipe, std::size_t i){
                    if (!pipe) {
                        if (useUnlink) rcx.unlink(keys[i]);
                        else           rcx.del(keys[i]);
                    } else {
                        if (useUnlink) pipe->unlink(keys[i]);
                        else           pipe->del(keys[i]);
                    }
                });
        }
        return;
    }

    // Mixed case: group by tag; untagged -> by slot.
    std::unordered_map<std::string_view, TagBucket> by_tag;
    std::vector<std::size_t> no_tag;
    by_tag.reserve(keys.size()); no_tag.reserve(keys.size());

    for (std::size_t i = 0; i < keys.size(); ++i) {
        std::string_view t = rc::extract_tag(keys[i]);
        if (t.empty()) no_tag.push_back(i);
        else           by_tag[t].rows.push_back(i);
    }

    // Tagged buckets
    for (auto& kv : by_tag) {
        auto& rows = kv.second.rows;
        if (rows.empty()) continue;

        if (rows.size() < pol.min_pipeline) {
            for (std::size_t p = 0; p < rows.size(); p += pol.batch_window) {
                const std::size_t upto = std::min(rows.size(), p + pol.batch_window);
                std::vector<std::string> win; win.reserve(upto - p);
                for (std::size_t j = p; j < upto; ++j) win.emplace_back(keys[rows[j]]);
                do_window_cmd(win.begin(), win.end());
            }
        } else {
            conn_.pipeline_windowed(std::string(kv.first), rows.size(), // TODO
                [&](sw::redis::Pipeline* pipe, std::size_t i){
                    const auto idx = rows[i];
                    if (!pipe) {
                        if (useUnlink) rcx.unlink(keys[idx]);
                        else           rcx.del(keys[idx]);
                    } else {
                        if (useUnlink) pipe->unlink(keys[idx]);
                        else           pipe->del(keys[idx]);
                    }
                });
        }
    }

    // Untagged: group by slot.
    std::unordered_map<int, SlotBucket> by_slot;
    by_slot.reserve(no_tag.size());
    for (auto i : no_tag) {
        const int slot = rc::key_slot(std::string_view{keys[i]});
        by_slot[slot].rows.push_back(i);
    }

    for (auto& kv : by_slot) {
        auto& rows = kv.second.rows;
        if (rows.empty()) continue;

        if (rows.size() < pol.min_pipeline) {
            for (std::size_t p = 0; p < rows.size(); p += pol.batch_window) {
                const std::size_t upto = std::min(rows.size(), p + pol.batch_window);
                std::vector<std::string> win; win.reserve(upto - p);
                for (std::size_t j = p; j < upto; ++j) win.emplace_back(keys[rows[j]]);
                do_window_cmd(win.begin(), win.end());
            }
        } else {
            const std::string& routeKey = keys[rows.front()];
            conn_.pipeline_windowed(routeKey, rows.size(),
                [&](sw::redis::Pipeline* pipe, std::size_t i){
                    const auto idx = rows[i];
                    if (!pipe) {
                        if (useUnlink) rcx.unlink(keys[idx]);
                        else           rcx.del(keys[idx]);
                    } else {
                        if (useUnlink) pipe->unlink(keys[idx]);
                        else           pipe->del(keys[idx]);
                    }
                });
        }
    }
}

}