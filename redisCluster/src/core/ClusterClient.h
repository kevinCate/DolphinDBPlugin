//
// Created by uplee on 9/25/25.
//

#ifndef CLUSTERCLIENT_H
#define CLUSTERCLIENT_H

#pragma once
#include "rc_connection.h"
#include "DolphinDBEverything.h"

class ClusterClient {
public:
    explicit ClusterClient(RedisClusterConn& holder) : holder_(holder) {}

    // --- KV ---
    sw::redis::OptionalString get(const std::string& key) const;
    void set(const std::string& key, const std::string& val) const;
    void setex(const std::string& key, const std::string& val, int ttl_sec) const;

    void mget(const std::vector<std::string>& keys, std::vector<sw::redis::OptionalString>& out) const;

    // 预留：hash / list / pipeline ...
    // void hset(...);
    // void lpush(...);
    // void pipelineSet(...);

    // === 新增：批量 HSET（每行多字段），带窗口 & hash-tag 分组，面向 Cluster 优化 ===
    void batchHashSet(const std::vector<std::string>& keys,
                      const ddb::TableSP& fieldData,             // 所有列必须为 STRING
                      std::size_t batchWin = 2048) const;
    void batchHashSetThread(const std::vector<std::string>& keys, const ddb::TableSP& fieldData, std::size_t batchWin) const;

    void deleteKeys(const std::vector<std::string>& keys, std::size_t batchWin, bool useUnlink) const;

private:
    RedisClusterConn& holder_;
};

// ClusterClient.h 里声明一个工作类
class RCSetWorker : public ddb::Runnable {
public:
    RCSetWorker(sw::redis::RedisCluster& rc,
                std::shared_ptr<const std::vector<std::string>> keys,
                std::shared_ptr<const std::vector<std::string>> fieldNames,
                std::shared_ptr<const std::vector<ddb::DolphinString*>> colData,
                int numCols,
                std::size_t batchWin,
                std::vector<std::size_t> rows,
                // routeKey 非空 => 单槽/同 tag，建议传 tag；为空 => 混槽或无 tag
                std::string routeKey,
                // 小批量直发阈值
                std::size_t minPipe)
    : rc_(rc), keys_(std::move(keys)), fieldNames_(std::move(fieldNames)),
      colData_(std::move(colData)), numCols_(numCols),
      batchWin_(batchWin), rows_(std::move(rows)),
      routeKey_(std::move(routeKey)), minPipe_(minPipe) {}

    void run() override {
        try {
            // 小块：直发；大块：pipeline(new_connection=true)
            if (rows_.size() < minPipe_) {
                std::vector<std::pair<sw::redis::StringView, sw::redis::StringView>> fvs;
                fvs.reserve(numCols_);
                for (auto row : rows_) {
                    fvs.clear();
                    for (int c = 0; c < numCols_; ++c) {
                        const auto& ds = (*colData_)[c][row];
                        const char* p  = ds.c_str();
                        const std::size_t n = std::strlen(p); // 若 DolphinString 有 size() 用它
                        fvs.emplace_back(sw::redis::StringView((*fieldNames_)[c]),
                                         sw::redis::StringView(p, n));
                    }
                    rc_.hset((*keys_)[row], fvs.begin(), fvs.end());
                }
                return;
            }

            // 大块：短 pipeline；new_connection=true 避免与其他线程争抢同连接
            std::size_t p = 0;
            while (p < rows_.size()) {
                auto upto = std::min(rows_.size(), p + batchWin_);

                // 把 pipeline 放到最小作用域，确保尽快释放连接（参考官方建议）
                {
                    auto pipe = rc_.pipeline(routeKey_, /*new_connection=*/true);

                    std::vector<std::pair<sw::redis::StringView, sw::redis::StringView>> fvs;
                    fvs.reserve(numCols_);
                    for (; p < upto; ++p) {
                        const auto row = rows_[p];
                        fvs.clear();
                        for (int c = 0; c < numCols_; ++c) {
                            const auto& ds = (*colData_)[c][row];
                            const char* vp = ds.c_str();
                            const std::size_t vn = std::strlen(vp);
                            fvs.emplace_back(sw::redis::StringView((*fieldNames_)[c]),
                                             sw::redis::StringView(vp, vn));
                        }
                        pipe.hset((*keys_)[row], fvs.begin(), fvs.end());
                    }

                    // 立刻提交并释放连接回池
                    pipe.exec();
                }
            }
        } catch (const std::exception& e) {
            // 记录第一条异常（如需，把它抛给主线程；这里简单打印/可用原子标记）
            // 在 DolphinDB 插件里可通过状态变量把错误回传给主线程
            throw;
        }
    }

private:
    sw::redis::RedisCluster& rc_;
    std::shared_ptr<const std::vector<std::string>> keys_;
    std::shared_ptr<const std::vector<std::string>> fieldNames_;
    std::shared_ptr<const std::vector<ddb::DolphinString*>> colData_;
    int numCols_;
    std::size_t batchWin_;
    std::vector<std::size_t> rows_;
    std::string routeKey_;
    std::size_t minPipe_;
};

#endif //CLUSTERCLIENT_H
