#ifndef HASH_BATCH_H
#define HASH_BATCH_H

// src/commands/batch.h
#pragma once
#include "ddbplugin/Plugin.h"
#include <vector>

ddb::ConstantSP ddb_rc_batchGet(ddb::Heap*, const std::vector<ddb::ConstantSP>&);
ddb::ConstantSP ddb_rc_batchHashSet(ddb::Heap*, const std::vector<ddb::ConstantSP>&);
ddb::ConstantSP ddb_rc_deleteKeys(ddb::Heap*, const std::vector<ddb::ConstantSP>&);

#endif //HASH_BATCH_H