//
// Created by uplee on 9/25/25.
//

#include "core/ClusterClient.h"
#include <sw/redis++/redis++.h>

static std::string extract_hashtag(const std::string& k){
    const auto l = k.find('{');
    if (l == std::string::npos) return {};
    const auto r = k.find('}', l + 1);
    if (r == std::string::npos || r == l + 1) return {}; // {} 或缺失右括号 -> 无效
    return k.substr(l + 1, r - (l + 1));
}

// DolphinString 取指针与长度的兼容层
static const char* ds_data(const ddb::DolphinString& ds){ return ds.c_str(); }
// 如果你的 DolphinString 有 size()/length()，优先用；这里保守退回到 strlen。
static std::size_t ds_size(const ddb::DolphinString& ds){
    const char* p = ds.c_str();
    return std::strlen(p);
}

// ---- CRC16/slot & hash-basis ----
// 多数客户端与 Redis 自身实现一致：CRC16-CCITT，多项式 0x1021，初始值 0。
// https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/#appendix-a-crc16-reference-implementation-in-ansi-c
static uint16_t crc16_xmodem(const uint8_t* p, size_t n) noexcept {
    static constexpr std::array<uint16_t,256> T = {
        0x0000,0x1021,0x2042,0x3063,0x4084,0x50a5,0x60c6,0x70e7,
        0x8108,0x9129,0xa14a,0xb16b,0xc18c,0xd1ad,0xe1ce,0xf1ef,
        0x1231,0x0210,0x3273,0x2252,0x52b5,0x4294,0x72f7,0x62d6,
        0x9339,0x8318,0xb37b,0xa35a,0xd3bd,0xc39c,0xf3ff,0xe3de,
        0x2462,0x3443,0x0420,0x1401,0x64e6,0x74c7,0x44a4,0x5485,
        0xa56a,0xb54b,0x8528,0x9509,0xe5ee,0xf5cf,0xc5ac,0xd58d,
        0x3653,0x2672,0x1611,0x0630,0x76d7,0x66f6,0x5695,0x46b4,
        0xb75b,0xa77a,0x9719,0x8738,0xf7df,0xe7fe,0xd79d,0xc7bc,
        0x48c4,0x58e5,0x6886,0x78a7,0x0840,0x1861,0x2802,0x3823,
        0xc9cc,0xd9ed,0xe98e,0xf9af,0x8948,0x9969,0xa90a,0xb92b,
        0x5af5,0x4ad4,0x7ab7,0x6a96,0x1a71,0x0a50,0x3a33,0x2a12,
        0xdbfd,0xcbdc,0xfbbf,0xeb9e,0x9b79,0x8b58,0xbb3b,0xab1a,
        0x6ca6,0x7c87,0x4ce4,0x5cc5,0x2c22,0x3c03,0x0c60,0x1c41,
        0xedae,0xfd8f,0xcdec,0xddcd,0xad2a,0xbd0b,0x8d68,0x9d49,
        0x7e97,0x6eb6,0x5ed5,0x4ef4,0x3e13,0x2e32,0x1e51,0x0e70,
        0xff9f,0xefbe,0xdfdd,0xcffc,0xbf1b,0xaf3a,0x9f59,0x8f78,
        0x9188,0x81a9,0xb1ca,0xa1eb,0xd10c,0xc12d,0xf14e,0xe16f,
        0x1080,0x00a1,0x30c2,0x20e3,0x5004,0x4025,0x7046,0x6067,
        0x83b9,0x9398,0xa3fb,0xb3da,0xc33d,0xd31c,0xe37f,0xf35e,
        0x02b1,0x1290,0x22f3,0x32d2,0x4235,0x5214,0x6277,0x7256,
        0xb5ea,0xa5cb,0x95a8,0x8589,0xf56e,0xe54f,0xd52c,0xc50d,
        0x34e2,0x24c3,0x14a0,0x0481,0x7466,0x6447,0x5424,0x4405,
        0xa7db,0xb7fa,0x8799,0x97b8,0xe75f,0xf77e,0xc71d,0xd73c,
        0x26d3,0x36f2,0x0691,0x16b0,0x6657,0x7676,0x4615,0x5634,
        0xd94c,0xc96d,0xf90e,0xe92f,0x99c8,0x89e9,0xb98a,0xa9ab,
        0x5844,0x4865,0x7806,0x6827,0x18c0,0x08e1,0x3882,0x28a3,
        0xcb7d,0xdb5c,0xeb3f,0xfb1e,0x8bf9,0x9bd8,0xabbb,0xbb9a,
        0x4a75,0x5a54,0x6a37,0x7a16,0x0af1,0x1ad0,0x2ab3,0x3a92,
        0xfd2e,0xed0f,0xdd6c,0xcd4d,0xbdaa,0xad8b,0x9de8,0x8dc9,
        0x7c26,0x6c07,0x5c64,0x4c45,0x3ca2,0x2c83,0x1ce0,0x0cc1,
        0xef1f,0xff3e,0xcf5d,0xdf7c,0xaf9b,0xbfba,0x8fd9,0x9ff8,
        0x6e17,0x7e36,0x4e55,0x5e74,0x2e93,0x3eb2,0x0ed1,0x1ef0
    };
    uint16_t c = 0;
    while (n--) c = static_cast<uint16_t>((c << 8) ^ T[((c >> 8) ^ *p++) & 0xFF]);
    return c;
}
// ----- hash tag 提取（只切片，不分配）-----
static std::string_view extract_tag_sv(std::string_view v) noexcept {
    const size_t l = v.find('{');
    if (l == std::string_view::npos) return {};
    const size_t r = v.find('}', l + 1);
    if (r == std::string_view::npos || r == l + 1) return {};
    return v.substr(l + 1, r - l - 1);
}

// 有 tag 用 tag；否则用整 key
static std::string_view hash_basis(std::string_view k) noexcept {
    const std::string_view t = extract_tag_sv(k);
    return t.empty() ? k : t;
}

// 计算 Redis Cluster 槽位
static int key_slot(std::string_view key) noexcept {
    const auto base = hash_basis(key);
    return crc16_xmodem(reinterpret_cast<const uint8_t*>(base.data()), base.size()) % 16384;
}

// --------------------- 任务定义 ---------------------
struct BucketTask {
    std::string routeKey;           // 有 tag: tag；无 tag: 代表 key 或空（仅用于 pipeline 的路由）
    std::vector<size_t> rows;       // 负责的行号
    bool usePipeline = false;       // 大桶用 pipeline，小桶直发
};

 sw::redis::OptionalString ClusterClient::get(const std::string& key) const
 {
    return holder_.get().get(key);
}

void ClusterClient::set(const std::string& key, const std::string& val) const
{
    holder_.get().set(key, val);
}

void ClusterClient::setex(const std::string& key, const std::string& val, int ttl_sec) const
{
    holder_.get().setex(key, std::chrono::seconds(ttl_sec), val);
}

void ClusterClient::mget(const std::vector<std::string>& keys,
                         std::vector<sw::redis::OptionalString>& out) const
{
    out.clear();
    out.resize(keys.size());
    if (keys.empty()) return;

    // 1) 如果所有 key 都使用了同一个非空 hash tag，直接一次 MGET
    const std::string tag0 = extract_hashtag(keys[0]);
    bool all_same_tag = !tag0.empty();
    for (size_t i = 1; i < keys.size(); ++i) {
        if (extract_hashtag(keys[i]) != tag0) { all_same_tag = false; break; }
    }

    auto& rc = holder_.get();   // <== 关键：拿到真正的 RedisCluster 实例引用

    if (all_same_tag) {
        rc.mget(keys.begin(), keys.end(), std::back_inserter(out));
        return;
    }

    // 2) 否则：按 tag 分组做多次 MGET；无 tag 的逐个 GET
    struct Bucket { std::vector<std::string> keys; std::vector<size_t> idx; };
    std::unordered_map<std::string, Bucket> groups;
    std::vector<size_t> no_tag_idx;
    groups.reserve(keys.size());

    for (size_t i = 0; i < keys.size(); ++i) {
        const auto& k = keys[i];
        std::string t = extract_hashtag(k);
        if (t.empty()) no_tag_idx.push_back(i);
        else {
            auto& b = groups[t];
            b.keys.push_back(k);
            b.idx.push_back(i);
        }
    }

    for (auto& kv : groups) {
        auto& b = kv.second;
        std::vector<sw::redis::OptionalString> tmp;
        tmp.reserve(b.keys.size());
        rc.mget(b.keys.begin(), b.keys.end(), std::back_inserter(tmp));
        for (size_t j = 0; j < b.idx.size(); ++j) {
            out[b.idx[j]] = std::move(tmp[j]);
        }
    }

    for (auto i : no_tag_idx) {
        out[i] = rc.get(keys[i]);
    }
}

void ClusterClient::batchHashSet(const std::vector<std::string>& keys,
                                 const ddb::TableSP& fieldData,
                                 std::size_t batchWin) const
{
    const auto N = static_cast<std::size_t>(fieldData->size());
    if (keys.size() != N)
        throw ddb::IllegalArgumentException("batchHashSet", "keys and fieldData must have same number of rows");

    const int numCols = fieldData->columns();
    if (numCols <= 0) return;

    // ---------- 1) 准备列名 & 列指针（一次性获取） ----------
    std::vector<std::string> fieldNames(numCols);
    std::vector<ddb::VectorSP> cols(numCols);
    std::vector<ddb::DolphinString*> colData(numCols, nullptr);
    for (int c = 0; c < numCols; ++c) {
        fieldNames[c] = fieldData->getColumnName(c);
        cols[c] = fieldData->getColumn(c);
        if (cols[c]->getType() != ddb::DT_STRING)
            throw ddb::RuntimeException("[Plugin::RedisCluster] fieldData columns must be STRING");
        // String 列：直接拿到底层数组指针（最快 & 稳）
        colData[c] = static_cast<ddb::DolphinString*>(cols[c]->getDataArray());
        if (!colData[c]) // 极端情况下返回空指针时，要兜底
            throw ddb::RuntimeException("[Plugin::RedisCluster] getDataArray() returned null for STRING column");
    }
    // 列名 -> StringView（常量，行内复用）
    std::vector<sw::redis::StringView> fnames(numCols);
    for (int c = 0; c < numCols; ++c) {
        const auto& s = fieldNames[c];
        fnames[c] = sw::redis::StringView(s.data(), s.size());
    }

    // ---------- 2) 按 hash-tag 分组 ----------
    struct Bucket { std::vector<std::size_t> idx; };
    std::unordered_map<std::string, Bucket> groups;
    std::vector<std::size_t> noTag;
    groups.reserve(N);

    for (std::size_t i = 0; i < N; ++i) {
        auto tag = extract_hashtag(keys[i]);
        (tag.empty() ? noTag : groups[tag].idx).push_back(i);
    }

    auto& cluster = holder_.get();

    // ---------- 3) 每个 tag 组：pipeline(new_connection=true) + 窗口提交 ----------
    constexpr std::size_t kMinPipe = 6;
    auto send_group = [&](const std::string& tag, const std::vector<std::size_t>& rows){
        if (rows.empty()) return;

        // Reuse this container in both paths
        std::vector<std::pair<sw::redis::StringView, sw::redis::StringView>> fvs;
        fvs.reserve(numCols);

        try {
         if (rows.size() < kMinPipe) {
             // Small batch: direct commands (no pipeline)
             for (std::size_t row : rows) {
                 fvs.clear();
                 for (int c = 0; c < numCols; ++c) {
                     const ddb::DolphinString& ds = colData[c][row];
                     const char* vptr = ds_data(ds);
                     const std::size_t vlen = ds_size(ds);
                     fvs.emplace_back(fnames[c], sw::redis::StringView(vptr, vlen));
                 }
                 cluster.hset(keys[row], fvs.begin(), fvs.end());
             }
             return;
         }

         // Large batch: short, windowed pipeline pinned to the tag (single slot)
         auto pipe = cluster.pipeline(tag, /*new_connection=*/false);
         for (std::size_t p = 0; p < rows.size(); ) {
             const std::size_t upto = std::min(rows.size(), p + batchWin);
             for (; p < upto; ++p) {
                 const std::size_t row = rows[p];
                 fvs.clear();
                 for (int c = 0; c < numCols; ++c) {
                     const ddb::DolphinString& ds = colData[c][row];
                     const char* vptr = ds_data(ds);
                     const std::size_t vlen = ds_size(ds);
                     fvs.emplace_back(fnames[c], sw::redis::StringView(vptr, vlen));
                 }
                 pipe.hset(keys[row], fvs.begin(), fvs.end());
             }
             pipe.exec();
         }
        } catch (const std::exception& e) {
         throw ddb::RuntimeException(
             std::string("[Plugin::RedisCluster] batchHashSet pipeline(tag=") + tag + ") failed: " + e.what());
        }
    };

    // 分组内下标一般已是递增；如有需要可排序保证单调，但这里直接按索引访问 colData 不要求连续。
    for (auto& kv : groups) send_group(kv.first, kv.second.idx);

    // 2) **无 tag**：按槽分组（小组直发，大组 pipeline）
    struct SlotBucket { std::vector<size_t> idx; };
    // 经验：桶数量 <= 16384，但通常远小于 N；用开放寻址的 flat_map 更好，先用 unordered_map 简化
    std::unordered_map<int, SlotBucket> by_slot;
    by_slot.reserve(noTag.size());

    for (size_t i : noTag) {
        const int slot = key_slot(std::string_view{keys[i]});
        by_slot[slot].idx.push_back(i);
    }

    // 阈值：小于此值走直发，避免 pipeline 固定成本
    std::vector<std::pair<std::string_view, std::string_view>> fvs;
    fvs.reserve(numCols);

    for (auto& kv : by_slot) {
        auto& rows = kv.second.idx;
        if (rows.size() < kMinPipe) {
            // 小组：逐条直发
            for (size_t row : rows) {
                fvs.clear();
                for (int c = 0; c < numCols; ++c) {
                    const auto& ds = colData[c][row];
                    const char* vptr = ds_data(ds);
                    const size_t vlen = ds_size(ds);
                    fvs.emplace_back(fnames[c], std::string_view(vptr, vlen));
                }
                cluster.hset(keys[row], fvs.begin(), fvs.end());
            }
        } else {
            // 大组：开短 pipeline（不传 tag，但全是同槽，不会触发 MOVED）
            // 这里传 *一个* 代表 key 的 tag/基串也可，但对无 tag 情况传空即可：
            const auto& routeKey = keys[rows.front()];  // representative key of this slot
            auto pipe = cluster.pipeline(routeKey, /*new_connection=*/false);

            for (size_t p = 0; p < rows.size();) {
                const size_t upto = std::min(rows.size(), p + batchWin);
                for (; p < upto; ++p) {
                    const size_t row = rows[p];
                    fvs.clear();
                    for (int c = 0; c < numCols; ++c) {
                        const auto& ds = colData[c][row];
                        const char* vptr = ds_data(ds);
                        const size_t vlen = ds_size(ds);
                        fvs.emplace_back(fnames[c], std::string_view(vptr, vlen));
                    }
                    pipe.hset(keys[row], fvs.begin(), fvs.end());
                }
                pipe.exec(); // 窗口提交
            }
        }
    }
}

void ClusterClient::batchHashSetThread(const std::vector<std::string>& keys,
                                 const ddb::TableSP& fieldData,
                                 std::size_t batchWin,
                                 int numThreads) const
{
    const auto N = static_cast<std::size_t>(fieldData->size());
    if (keys.size() != N)
        throw ddb::IllegalArgumentException(__FUNCTION__, "keys and fieldData must have same number of rows");
    const int numCols = fieldData->columns();
    if (numCols <= 0) return;

    // ---- 1) 读列名与零拷贝列数据（一次性）----
    auto fieldNames = std::make_shared<std::vector<std::string>>(numCols);
    auto cols       = std::vector<ddb::VectorSP>(numCols);
    auto colData    = std::make_shared<std::vector<ddb::DolphinString*>>(numCols, nullptr);
    for (int c = 0; c < numCols; ++c) {
        (*fieldNames)[c] = fieldData->getColumnName(c);
        cols[c] = fieldData->getColumn(c);
        if (cols[c]->getType() != ddb::DT_STRING)
            throw ddb::RuntimeException("[Plugin::RedisCluster] fieldData columns must be STRING");
        (*colData)[c] = static_cast<ddb::DolphinString*>(cols[c]->getDataArray());
        if (!(*colData)[c])
            throw ddb::RuntimeException("[Plugin::RedisCluster] getDataArray() returned null");
    }

    // ---- 2) 按 tag 分两类：有 tag 组（tag -> rows），无 tag 列表 ----
    struct TagBucket { std::vector<std::size_t> rows; };
    std::unordered_map<std::string, TagBucket> tagged;
    std::vector<std::size_t> noTag;
    tagged.reserve(N); noTag.reserve(N);

    for (std::size_t i = 0; i < N; ++i) {
        auto t = extract_hashtag(keys[i]);
        if (t.empty()) noTag.push_back(i);
        else           tagged[t].rows.push_back(i);
    }

    // ---- 3) 生成任务列表：有 tag 的一组一个任务；无 tag 的按顺序切块 ----
    constexpr std::size_t kMinPipe = 6;                            // 小组直发阈值
    const std::size_t chunk = std::max<std::size_t>(batchWin, kMinPipe); // 无 tag 切块大小

    std::vector<BucketTask> tasks;
    tasks.reserve(tagged.size() + (noTag.size() + chunk - 1) / chunk);

    // A) tagged groups -> one task each
    for (auto &kv : tagged) {
        if (kv.second.rows.empty()) continue;
        BucketTask t;
        t.routeKey   = kv.first;           // 用 tag 当作 routeKey（同槽）
        t.rows       = std::move(kv.second.rows);
        t.usePipeline= true;
        tasks.emplace_back(std::move(t));
    }

    // B) **no tag** -> group by SLOT first
    struct SlotBucket { std::vector<std::size_t> rows; };
    std::unordered_map<int, SlotBucket> by_slot;
    by_slot.reserve(noTag.size());
    for (std::size_t i : noTag) {
        const int slot = key_slot(std::string_view{keys[i]});
        by_slot[slot].rows.push_back(i);
    }

    for (auto &kv : by_slot) {
        auto &rows = kv.second.rows;
        if (rows.empty()) continue;
        BucketTask t;
        t.rows        = std::move(rows);
        t.usePipeline = (t.rows.size() >= kMinPipe);
        // bind pipeline to a representative **key** so redis-plus-plus pins to the slot
        t.routeKey    = keys[t.rows.front()];
        tasks.emplace_back(std::move(t));
    }

    // ---- 4) 线程并发度：不超过硬件并发和任务数（也可做配置）----
    auto& rc = holder_.get(); // RedisCluster
    const unsigned hw = std::max(1u, std::thread::hardware_concurrency());

    // sanitize requested threads; default to 3 if <= 0
    int desired = (numThreads <= 0 ? 3 : numThreads);
    int threads = std::max(1, std::min<int>(desired, static_cast<int>(hw)));

    if (!tasks.empty())
        threads = std::min<int>(threads, static_cast<int>(tasks.size()));
    else
        threads = 1;

    std::vector<std::vector<BucketTask>> shards(threads);
    for (std::size_t i = 0; i < tasks.size(); ++i)
        shards[i % threads].emplace_back(std::move(tasks[i]));

    // ---- 5) 启线程（DolphinDB ThreadSP），每个线程串行处理自己的任务片 ----
    auto spKeys = std::make_shared<const std::vector<std::string>>(keys);
    auto spNames= std::make_shared<const std::vector<std::string>>(*fieldNames);

    std::vector<ddb::ThreadSP> ths;
    ths.reserve(tasks.size()); // 精确容量

    for (int i = 0; i < threads; ++i) {
        for (auto &task : shards[i]) {
            // 交由 Thread 接管生命周期；本地不再持有“可析构所有权”
            auto* w = new RCSetWorker(rc, spKeys, spNames, colData,
                                          numCols, batchWin,
                                          std::move(task.rows), task.routeKey, kMinPipe);
            ddb::ThreadSP t = new ddb::Thread(w);   // 唯一所有者
            if (!t->isStarted()) t->start();
            ths.emplace_back(std::move(t));
        }
    }
    for (auto &t : ths) t->join();  // 线程收尾时会正确清理 runnable
}

void ClusterClient::deleteKeys(const std::vector<std::string>& keys,
                               std::size_t batchWin,
                               bool useUnlink) const
{
    if (keys.empty()) return;

    auto& rc = holder_.get();

    // ---- fast path: all keys share SAME non-empty hash tag -> single-slot
    const std::string tag0 = extract_hashtag(keys[0]);
    bool all_same_tag = !tag0.empty();
    for (size_t i = 1; i < keys.size() && all_same_tag; ++i)
        if (extract_hashtag(keys[i]) != tag0) all_same_tag = false;

    // small buckets -> one multi-key cmd; big buckets -> short pipeline
    constexpr std::size_t kMinPipe = 6;  // keep consistent with batchHashSet

    auto do_window_cmd = [&](auto first, auto last){
        if (useUnlink) rc.unlink(first, last);
        else           rc.del(first, last);
    };

    if (all_same_tag) {
        const size_t n = keys.size();
        if (n < kMinPipe) {
            for (size_t p = 0; p < n; p += batchWin) {
                const size_t upto = std::min(n, p + batchWin);
                do_window_cmd(keys.begin() + p, keys.begin() + upto);
            }
        } else {
            // bind pipeline to tag so all ops go to the same slot/node
            auto pipe = rc.pipeline(tag0, /*new_connection=*/false);
            for (size_t p = 0; p < n; ) {
                const size_t upto = std::min(n, p + batchWin);
                for (; p < upto; ++p) {
                    if (useUnlink) pipe.unlink(keys[p]);
                    else           pipe.del(keys[p]);
                }
                pipe.exec();
            }
        }
        return;
    }

    // ---- mixed case: group by tag; no-tag -> group by slot
    struct Bucket { std::vector<size_t> idx; };
    std::unordered_map<std::string, Bucket> by_tag;
    std::vector<size_t> no_tag;
    by_tag.reserve(keys.size()); no_tag.reserve(keys.size());

    for (size_t i = 0; i < keys.size(); ++i) {
        std::string t = extract_hashtag(keys[i]);
        if (t.empty()) no_tag.push_back(i);
        else           by_tag[t].idx.push_back(i);
    }

    // -- tagged buckets (already same slot by definition) --
    for (auto &kv : by_tag) {
        auto &rows = kv.second.idx;
        if (rows.empty()) continue;
        if (rows.size() < kMinPipe) {
            for (size_t p = 0; p < rows.size(); p += batchWin) {
                const size_t upto = std::min(rows.size(), p + batchWin);
                // build contiguous window of keys
                std::vector<std::string> win; win.reserve(upto - p);
                for (size_t j = p; j < upto; ++j) win.emplace_back(keys[rows[j]]);
                do_window_cmd(win.begin(), win.end());
            }
        } else {
            auto pipe = rc.pipeline(kv.first, /*new_connection=*/false);
            for (size_t p = 0; p < rows.size(); ) {
                const size_t upto = std::min(rows.size(), p + batchWin);
                for (; p < upto; ++p) {
                    const auto &k = keys[rows[p]];
                    if (useUnlink) pipe.unlink(k);
                    else           pipe.del(k);
                }
                pipe.exec();
            }
        }
    }

    // -- no-tag: group by SLOT to avoid CROSSSLOT; optionally short pipeline
    struct SlotBucket { std::vector<size_t> idx; };
    std::unordered_map<int, SlotBucket> by_slot;
    by_slot.reserve(no_tag.size());

    for (size_t i : no_tag) {
        const int slot = key_slot(std::string_view{keys[i]});
        by_slot[slot].idx.push_back(i);
    }

    for (auto &kv : by_slot) {
        auto &rows = kv.second.idx;
        if (rows.empty()) continue;

        if (rows.size() < kMinPipe) {
            for (size_t p = 0; p < rows.size(); p += batchWin) {
                const size_t upto = std::min(rows.size(), p + batchWin);
                std::vector<std::string> win; win.reserve(upto - p);
                for (size_t j = p; j < upto; ++j) win.emplace_back(keys[rows[j]]);
                do_window_cmd(win.begin(), win.end());
            }
        } else {
            // same-slot group -> safe to send via one connection
            const auto& routeKey = keys[rows.front()];  // representative key of this slot
            auto pipe = rc.pipeline(routeKey, /*new_connection=*/false);
            for (size_t p = 0; p < rows.size(); ) {
                const size_t upto = std::min(rows.size(), p + batchWin);
                for (; p < upto; ++p) {
                    const auto &k = keys[rows[p]];
                    if (useUnlink) pipe.unlink(k);
                    else           pipe.del(k);
                }
                pipe.exec();
            }
        }
    }
}