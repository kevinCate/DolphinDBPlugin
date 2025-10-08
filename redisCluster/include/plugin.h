//
// Created by uplee on 9/23/25.
//

#ifndef PLUGIN_H
#define PLUGIN_H

#pragma once
#include "DolphinDBEverything.h"
#include "ddbplugin/CommonInterface.h"

extern "C" ddb::ConstantSP redisClusterConnect(ddb::Heap* heap, const std::vector<ddb::ConstantSP>& args); // // args[0]=host(string), args[1]=port(int), 可选 args[2]=password(string), 可选 args[3]=poolSize(int), 可选 args[4]=readFromReplica(bool)
extern "C" ddb::ConstantSP redisClusterRelease(ddb::Heap* heap, const std::vector<ddb::ConstantSP>& args);
extern "C" ddb::ConstantSP redisClusterReleaseAll(ddb::Heap* heap, const std::vector<ddb::ConstantSP>& args);
extern "C" ddb::ConstantSP redisClusterGetHandle(ddb::Heap* heap, const std::vector<ddb::ConstantSP>& args);
extern "C" ddb::ConstantSP redisClusterGetHandleStatus(ddb::Heap* heap, const std::vector<ddb::ConstantSP>& args);
extern "C" ddb::ConstantSP redisClusterRun(ddb::Heap*, const std::vector<ddb::ConstantSP>& args);
extern "C" ddb::ConstantSP redisClusterSet(ddb::Heap*, const std::vector<ddb::ConstantSP>& args);
extern "C" ddb::ConstantSP redisClusterBatchSet(ddb::Heap*, const std::vector<ddb::ConstantSP>& args);
extern "C" ddb::ConstantSP redisClusterBatchHashSet(ddb::Heap*, const std::vector<ddb::ConstantSP>& args);
extern "C" ddb::ConstantSP redisClusterGet(ddb::Heap*, const std::vector<ddb::ConstantSP>& args);
extern "C" ddb::ConstantSP redisClusterMget(ddb::Heap*, const std::vector<ddb::ConstantSP>& args);
extern "C" ddb::ConstantSP redisClusterBatchDel(ddb::Heap*, const std::vector<ddb::ConstantSP>& args);
extern "C" ddb::ConstantSP redisClusterBatchPush(ddb::Heap*, const std::vector<ddb::ConstantSP>& args);
#endif //PLUGIN_H
