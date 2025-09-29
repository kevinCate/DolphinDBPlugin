//
// Created by uplee on 9/25/25.
//

// src/commands/hash_batch.cpp
#include "hash_batch.h"
#include "core/Globals.h"         // 提供 getConn(...) 或 g_rc_map + 常量
#include "core/ClusterClient.h"
#include <string>
#include <vector>

using ddb::ConstantSP;
using ddb::VectorSP;
using ddb::TableSP;

static inline std::vector<std::string> toStdStringVec(const VectorSP& sv){
    const auto n = sv->size();
    std::vector<std::string> out;
    out.reserve(n);
    for (int i = 0; i < n; ++i) out.emplace_back(sv->getString(i));
    return out;
}

ddb::ConstantSP ddb_rc_batchHashSet(ddb::Heap*, const std::vector<ddb::ConstantSP>& args){
    // args[0] = handle, args[1] = STRING vector (keys), args[2] = TABLE (all STRING)
    if (args.size() < 3)
        throw ddb::IllegalArgumentException(__FUNCTION__, "Usage: batchHashSet(conn, ids:STRING VECTOR, fieldData:STRING TABLE, batchWin:INT SCALAR)");

    // 句柄检查
    auto conn = getConn(args[0]);     // 由 core/Globals.h 提供，返回 SmartPointer<RedisClusterConn>
    if (args[1]->getForm() != ddb::DF_VECTOR || args[1]->getType() != ddb::DT_STRING)
        throw ddb::IllegalArgumentException(__FUNCTION__, "Argument ids must be STRING VECTOR.");
    if (!args[2]->isTable() || ((ddb::Table*)args[2].get())->getTableType() != ddb::BASICTBL)
        throw ddb::IllegalArgumentException(__FUNCTION__, "Argument fieldData must be a BASIC TABLE.");

    std::size_t batchWin = 2048;  // 默认值
    if (args.size() >= 4) {
        auto &bw = args[3];
        if (!bw->isScalar() || bw->getType() != ddb::DT_INT) {
            throw ddb::IllegalArgumentException(__FUNCTION__, "Argument batchWin must be an INT SCALAR if provided. Got type=" + ddb::Util::getDataTypeString(bw->getType()) + ", value=" + bw->getString() + ".");
        }
        batchWin = static_cast<std::size_t>(bw->getInt());
        if (batchWin == 0)
            throw ddb::IllegalArgumentException(__FUNCTION__, "Argument batchWin must be > 0.");
    }

    const auto ids = toStdStringVec(args[1]);
    const auto tb  = TableSP(args[2]);

    if (static_cast<std::size_t>(tb->size()) != ids.size())
        throw ddb::IllegalArgumentException(__FUNCTION__, "ids and fieldData must have the same number of rows.");

    // 这里可以做一次快捷检查：全部列必须为 STRING
    for (int c = 0; c < tb->columns(); ++c)
        if (tb->getColumn(c)->getType() != ddb::DT_STRING)
            throw ddb::RuntimeException("[Plugin::RedisCluster] fieldData columns must be STRING.");

    // 调用核心实现
    ClusterClient cli(*conn);
    cli.batchHashSet(ids, tb, /*batchWin*/ batchWin);

    return new ddb::String("batchHashSet finish.");
}


