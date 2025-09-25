//
// Created by uplee on 9/23/25.
//

#ifndef PLUGIN_H
#define PLUGIN_H

#pragma once
#include "DolphinDBEverything.h"
#include "ddbplugin/CommonInterface.h"

using ddb::ConstantSP;
using ddb::Heap;
using std::vector;

extern "C" ConstantSP redisClusterConnect(Heap* heap, const vector<ConstantSP>& args); // // args[0]=host(string), args[1]=port(int), 可选 args[2]=password(string), 可选 args[3]=poolSize(int), 可选 args[4]=readFromReplica(bool)
extern "C" ConstantSP redisClusterClose(Heap* heap, const vector<ConstantSP>& args);
extern "C" ddb::ConstantSP redisClusterGet(ddb::Heap*, const std::vector<ddb::ConstantSP>& args);
extern "C" ddb::ConstantSP redisClusterSet(ddb::Heap*, const std::vector<ddb::ConstantSP>& args);
extern "C" ddb::ConstantSP redisClusterMget(ddb::Heap*, const std::vector<ddb::ConstantSP>& args);

#endif //PLUGIN_H
