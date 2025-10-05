// src/core/ConnFacade.h (integrate connection + pipeline execution)
#ifndef CONNFACADE_H
#define CONNFACADE_H
#pragma once
#include "rc_connection.h"
#include "PipelinePolicy.h"
#include <sw/redis++/redis++.h>

namespace rc {

// TODO: Split logic
class ConnFacade {
public:
    explicit ConnFacade(RedisClusterConn& holder, const PipelinePolicy& pol = {})
    : holder_(holder), policy_(pol) {}

    [[nodiscard]] sw::redis::RedisCluster& rc() const { return holder_.get(); }

    [[nodiscard]] const PipelinePolicy& policy() const { return policy_; }

    void setPolicy(const PipelinePolicy& p) { policy_ = p; }
    void setBatchWindow(std::size_t n) { policy_.batch_window = n; }
    void setArgBatch(std::size_t n)    { policy_.arg_batch = n; }
    void setMinPipeline(std::size_t n) { policy_.min_pipeline = n; }
    void setNewConnection(bool b) { policy_.new_connection = b; }

private:
    RedisClusterConn& holder_;
    PipelinePolicy policy_;
};

} // namespace rc
#endif // CONNFACADE_H