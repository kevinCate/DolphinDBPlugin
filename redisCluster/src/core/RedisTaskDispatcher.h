#ifndef REDISTASKDISPATCHER_H
#define REDISTASKDISPATCHER_H
#pragma once

#include <vector>
#include <string>
#include <functional>
#include <memory>
#include "core/ConnFacade.h"
#include <sw/redis++/redis++.h>  // for Pipeline

namespace rc {

// Command-centric tasks (not key-aware)
struct CommandTask {
    std::string routeKey; // tag or representative key of slot
    std::function<void(sw::redis::RedisCluster&)> execDirect; // append a single logical command on direct conn
    std::function<void(sw::redis::Pipeline&)>    execPiped;   // append one logical command to an existing pipeline
};

void dispatchCommandTasks(rc::ConnFacade& conn,
    std::vector<CommandTask> tasks,
    int numThreads);

} // namespace rc

#endif // REDISTASKDISPATCHER_H