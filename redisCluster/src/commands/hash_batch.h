#ifndef HASH_BATCH_H
#define HASH_BATCH_H

// src/commands/hash_batch.h
#pragma once
#include "DolphinDBEverything.h"
#include <vector>

ddb::ConstantSP ddb_rc_mget(ddb::Heap*, const std::vector<ddb::ConstantSP>&);
ddb::ConstantSP ddb_rc_batchHashSet(ddb::Heap*, const std::vector<ddb::ConstantSP>&);
ddb::ConstantSP ddb_rc_batchHashSetThread(ddb::Heap*, const std::vector<ddb::ConstantSP>&);
ddb::ConstantSP ddb_rc_deleteKeys(ddb::Heap*, const std::vector<ddb::ConstantSP>&);

#endif //HASH_BATCH_H