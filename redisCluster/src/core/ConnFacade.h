// src/core/ConnFacade.h (integrate connection + pipeline execution)
#ifndef CONNFACADE_H
#define CONNFACADE_H
#pragma once
#include "rc_connection.h"
#include "PipelinePolicy.h"
#include <sw/redis++/redis++.h>
#include <algorithm>
#include <string>

namespace rc {

class ConnFacade {
public:
    explicit ConnFacade(RedisClusterConn& holder, const PipelinePolicy& pol = {})
    : holder_(holder), policy_(pol) {}

    [[nodiscard]] sw::redis::RedisCluster& rc() const { return holder_.get(); }

    [[nodiscard]] const PipelinePolicy& policy() const { return policy_; }
    void setPolicy(const PipelinePolicy& p) { policy_ = p; }
    void setBatchWindow(std::size_t n) { policy_.batch_window = n; }
    void setMinPipeline(std::size_t n) { policy_.min_pipeline = n; }
    void setNewConnection(bool b) { policy_.new_connection = b; }

    // Execute a single-slot group (same tag/slot) via windowed pipeline
    // fill_one_cmd(pipe*, idx) should emit one command; pipe==nullptr means send direct (small path).
    template <typename FillOneCmdFn>
    void pipeline_windowed(const std::string& routeKey,
                           std::size_t total,
                           FillOneCmdFn&& fill_one_cmd) const {
        if (total < policy_.min_pipeline) {
            for (std::size_t i = 0; i < total; ++i) fill_one_cmd(nullptr, i);
            return;
        }
        std::size_t p = 0;
        while (p < total) {
            const std::size_t upto = std::min(total, p + policy_.batch_window);
            auto pipe = rc().pipeline(routeKey, /*new_connection=*/policy_.new_connection);
            for (; p < upto; ++p) fill_one_cmd(&pipe, p);
            pipe.exec();
        }
    }

    // Small/direct path helper: send one command at a time. # TODO: Not in use
    template <typename SendOneCmdFn>
    void direct_small(std::size_t total, SendOneCmdFn&& send_one_cmd) const {
        for (std::size_t i = 0; i < total; ++i) send_one_cmd(i);
    }
private:
    RedisClusterConn& holder_;
    PipelinePolicy policy_;
};

} // namespace rc
#endif // CONNFACADE_H