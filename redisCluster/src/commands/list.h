//
// Created by uplee on 10/8/25.
//

#ifndef LIST_BATCH_H
#define LIST_BATCH_H
#pragma once
#include "ddbplugin/Plugin.h"
#include <vector>

// batchPush(conn, keys:STRING VECTOR, vals: VECTOR-of-STRING-VECTOR,
//           [rightPush:BOOL=true], [batchWin:INT=policy.default], [numThreads:INT=1])
ddb::ConstantSP ddb_rc_batchPush(ddb::Heap*, const std::vector<ddb::ConstantSP>&);

#endif // LIST_BATCH_H
