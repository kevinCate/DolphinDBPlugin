// commands/run.h
//
// Created by uplee on 10/6/25.
//

#ifndef DDB_REDIS_CLUSTER_COMMANDS_RUN_H_
#define DDB_REDIS_CLUSTER_COMMANDS_RUN_H_
#pragma once

#include "ddbplugin/Plugin.h"
#include <vector>

// Run a Redis command on a routed node.
// argv must be a STRING VECTOR: [routeKey, COMMAND, arg1, ...]
ddb::ConstantSP ddb_rc_run(ddb::Heap*, const std::vector<ddb::ConstantSP>& args);

#endif  // DDB_REDIS_CLUSTER_COMMANDS_RUN_H_