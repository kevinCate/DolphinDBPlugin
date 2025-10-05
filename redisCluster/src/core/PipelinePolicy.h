//
// Created by uplee on 10/2/25.
//

#ifndef PIPELINEPOLICY_H
#define PIPELINEPOLICY_H
#pragma once
#include <cstddef>

namespace rc {

struct PipelinePolicy {
    std::size_t batch_window = 2048;  // commands per exec()
    std::size_t arg_batch    = 1024;  // arguments per single command (e.g. keys per DEL)

    std::size_t min_pipeline = 6;     // below this, send direct commands
    bool new_connection = false;      // true for long multi-threaded runs
};

} // namespace rc

#endif // PIPELINEPOLICY_H
