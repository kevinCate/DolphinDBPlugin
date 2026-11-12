//
// Created by uplee on 9/25/25.
//

#ifndef DDB_REDIS_CLUSTER_COMMANDS_KV_H_
#define DDB_REDIS_CLUSTER_COMMANDS_KV_H_

#pragma once
#include "DolphinDBEverything.h"

ddb::ConstantSP ddb_rc_get(ddb::Heap*, const std::vector<ddb::ConstantSP>&);
ddb::ConstantSP ddb_rc_set(ddb::Heap*, const std::vector<ddb::ConstantSP>&);
ddb::ConstantSP ddb_rc_mget(ddb::Heap*, const std::vector<ddb::ConstantSP>&);

#endif //DDB_REDIS_CLUSTER_COMMANDS_KV_H_
