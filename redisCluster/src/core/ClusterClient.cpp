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
inline const char* ds_data(const ddb::DolphinString& ds){ return ds.c_str(); }
// 如果你的 DolphinString 有 size()/length()，优先用；这里保守退回到 strlen。
inline std::size_t ds_size(const ddb::DolphinString& ds){
    const char* p = ds.c_str();
    return std::strlen(p);
}

 sw::redis::OptionalString ClusterClient::get(const std::string& key){
    return holder_.get().get(key);
}

void ClusterClient::set(const std::string& key, const std::string& val){
    holder_.get().set(key, val);
}

void ClusterClient::setex(const std::string& key, const std::string& val, int ttl_sec){
    holder_.get().setex(key, std::chrono::seconds(ttl_sec), val);
}

void ClusterClient::mget(const std::vector<std::string>& keys,
                         std::vector<sw::redis::OptionalString>& out) {
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
                                 ddb::TableSP fieldData,
                                 std::size_t batchWin) {
    const std::size_t N = static_cast<std::size_t>(fieldData->size());
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
        colData[c] = reinterpret_cast<ddb::DolphinString*>(cols[c]->getDataArray());
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
    auto send_group = [&](const std::string& tag, const std::vector<std::size_t>& rows){
        if (rows.empty()) return;

        // 为了稳定性，优先使用 new_connection=true，避免与连接池中其他会话竞争。
        auto pipe = cluster.pipeline(tag, /*new_connection=*/false);

        // 行内复用容器：<field, value> 对
        std::vector<std::pair<sw::redis::StringView, sw::redis::StringView>> fvs;
        fvs.reserve(numCols);

        try {
            for (std::size_t p = 0; p < rows.size(); ) {
                const std::size_t upto = std::min(rows.size(), p + batchWin);

                for (; p < upto; ++p) {
                    const std::size_t row = rows[p];
                    fvs.clear();

                    // 直接用 colData[c][row] -> DolphinString
                    for (int c = 0; c < numCols; ++c) {
                        const ddb::DolphinString& ds = colData[c][row];
                        const char* vptr = ds_data(ds);
                        const std::size_t vlen = ds_size(ds);
                        fvs.emplace_back(fnames[c], sw::redis::StringView(vptr, vlen));
                    }
                    // 多字段 HSET（你也可以改成 hmset）
                    pipe.hset(keys[row], fvs.begin(), fvs.end());
                }

                pipe.exec();  // 提交该窗口
            }
        } catch (const std::exception& e) {
            throw ddb::RuntimeException(
                std::string("[Plugin::RedisCluster] batchHashSet pipeline(tag=") + tag + ") failed: " + e.what());
        }
    };

    // 分组内下标一般已是递增；如有需要可排序保证单调，但这里直接按索引访问 colData 不要求连续。
    for (auto& kv : groups) send_group(kv.first, kv.second.idx);

    // ---------- 4) 无 tag：逐条（后续可扩展：算槽再分组） ----------
    for (auto i : noTag) {
        std::vector<std::pair<sw::redis::StringView, sw::redis::StringView>> fvs;
        fvs.reserve(numCols);
        for (int c = 0; c < numCols; ++c) {
            const ddb::DolphinString& ds = colData[c][i];
            const char* vptr = ds_data(ds);
            const std::size_t vlen = ds_size(ds);
            fvs.emplace_back(fnames[c], sw::redis::StringView(vptr, vlen));
        }
        cluster.hset(keys[i], fvs.begin(), fvs.end());
    }
}