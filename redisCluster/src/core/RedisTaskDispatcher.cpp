#include "RedisTaskDispatcher.h"
#include <unordered_map>
#include "core/Router.h"

namespace {

int hardwareCap() {
    unsigned hc = std::thread::hardware_concurrency();
    return hc == 0 ? 1 : static_cast<int>(hc);
}

}

namespace rc {

struct SlotBucket { std::vector<std::size_t> rows; };
struct TagBucket  { std::vector<std::size_t> rows; };

class CommandTaskWorker : public ddb::Runnable {
public:
    using Group = std::pair<std::string, std::vector<CommandTask>>;

    CommandTaskWorker(ConnFacade& conn,
    std::vector<Group> items)
    : conn_(conn), items_(std::move(items)) {}
    void run() override {
        auto& rcx = conn_.rc();
        const auto& pol = conn_.policy();
        for (auto& kv : items_) {
            auto& routeKey = kv.first;
            auto& list = kv.second;
            if (list.size() < pol.min_pipeline) {
                for (auto& t : list) t.execDirect(rcx);
                continue;
            }
            size_t p = 0, n = list.size();
            while (p < n) {
                size_t upto = std::min(n, p + pol.batch_window);
                auto pipe = rcx.pipeline(routeKey, /*new_connection=*/true);
                for (; p < upto; ++p) list[p].execPiped(pipe);
                pipe.exec();
            }
        }
    }
private:
    ConnFacade& conn_;
    std::vector<Group> items_;
};

void dispatchCommandTasks(ConnFacade& conn,
    std::vector<CommandTask> tasks,
    int numThreads) {
    if (tasks.empty()) return;

    // Group by routeKey (hash-tag).
    std::unordered_map<std::string, std::vector<CommandTask>> groups;
    groups.reserve(tasks.size());
    for (auto& t : tasks) {
        groups[t.routeKey].push_back(std::move(t));
    }

    // Inline path: no extra threads requested.
    if (numThreads <= 1) {
        auto& rcx = conn.rc();
        const auto& pol = conn.policy();

        for (auto& kv : groups) {
            const std::string& routeKey = kv.first;
            auto& list = kv.second;

            if (list.size() < pol.min_pipeline) {
                for (auto& ct : list) ct.execDirect(rcx);
                continue;
            }

            std::size_t p = 0, n = list.size();
            while (p < n) {
                const std::size_t upto = std::min(n, p + pol.batch_window);
                // Honor configured policy in single-thread path.
                auto pipe = rcx.pipeline(routeKey, /*new_connection=*/pol.new_connection);
                for (; p < upto; ++p) {
                    list[p].execPiped(pipe);
                }
                pipe.exec();
            }
        }
        return;
    }

    // Multithreaded path with a hard cap.
    const int threadsCap = std::min(numThreads, hardwareCap());
    std::vector<ddb::ThreadSP> ths;

    if (groups.size() == 1) {
        // All commands hit the same routeKey (same hash-tag/slot).
        // Partition the single list into <= threadsCap shards and run CommandTaskWorker for each.
        auto it = groups.begin();
        const std::string routeKey = it->first;
        auto& list = it->second;
        const auto& pol = conn.policy();

        // Determine a reasonable shard count that never exceeds threadsCap.
        // Avoid creating shards smaller than min_pipeline when possible.
        const int maxByWork = std::max<int>(1, static_cast<int>(list.size() / std::max<std::size_t>(1, pol.min_pipeline)));
        const int shardCount = std::max(1, std::min(threadsCap, maxByWork));

        using Group = CommandTaskWorker::Group;
        std::vector<std::vector<Group>> shards(static_cast<std::size_t>(shardCount));

        // Split the single routeKey's tasks into shardCount parts (round-robin).
        std::vector<std::vector<CommandTask>> parts(static_cast<std::size_t>(shardCount));
        for (std::size_t i = 0; i < list.size(); ++i) {
            parts[i % static_cast<std::size_t>(shardCount)].push_back(std::move(list[i]));
        }
        for (int s = 0; s < shardCount; ++s) {
            if (!parts[static_cast<std::size_t>(s)].empty()) {
                shards[static_cast<std::size_t>(s)].emplace_back(routeKey,
                                                                 std::move(parts[static_cast<std::size_t>(s)]));
            }
        }

        ths.reserve(static_cast<std::size_t>(shardCount));
        for (int s = 0; s < shardCount; ++s) {
            auto* w = new CommandTaskWorker(conn, std::move(shards[static_cast<std::size_t>(s)]));
            ddb::ThreadSP thr = new ddb::Thread(w);
            thr->start();
            ths.emplace_back(std::move(thr));
        }
        for (auto& th : ths) th->join();
        return;
    }

    // Multiple routeKeys: distribute groups across <= threadsCap shards.
    const int shardCount = std::min<int>(threadsCap, static_cast<int>(groups.size()));
    using Group = CommandTaskWorker::Group;
    std::vector<std::vector<Group>> shards(static_cast<std::size_t>(shardCount));

    int idx = 0;
    for (auto& kv : groups) {
        shards[static_cast<std::size_t>(idx % shardCount)].emplace_back(kv.first,
                                                                        std::move(kv.second));
        ++idx;
    }

    ths.reserve(static_cast<std::size_t>(shardCount));
    for (int s = 0; s < shardCount; ++s) {
        auto* w = new CommandTaskWorker(conn, std::move(shards[static_cast<std::size_t>(s)]));
        ddb::ThreadSP thr = new ddb::Thread(w);
        thr->start();
        ths.emplace_back(std::move(thr));
    }
    for (auto& th : ths) th->join();
    }
} // namespace rc