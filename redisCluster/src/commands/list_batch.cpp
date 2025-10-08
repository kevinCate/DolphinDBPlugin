//
// Created by uplee on 10/8/25.
//

#include "commands/list_batch.h"
#include "core/ConnFacade.h"
#include "core/Globals.h"
#include "services/ListService.h"

using ddb::ConstantSP;
using ddb::IllegalArgumentException;
using ddb::RuntimeException;
using ddb::VectorSP;

namespace {

void parse_args(const std::vector<ConstantSP>& args,
    std::vector<std::string>& keys,
    std::vector<ddb::VectorSP>& vals,
    bool& rightPush,
    std::size_t& batchWin,
    int& numThreads) {
    // args[0] = handle already validated by caller of getConn
    if (args.size() < 3)
        throw IllegalArgumentException(__FUNCTION__,
        "[Plugin::RedisCluster] Usage: batchPush(conn, keys, values, [rightPush=true], [batchWin], [numThreads])");

    // keys: STRING VECTOR
    if (args[1]->getForm() != ddb::DF_VECTOR || args[1]->getType() != ddb::DT_STRING)
        throw IllegalArgumentException(__FUNCTION__, "[Plugin::RedisCluster] keys must be a STRING vector.");

    // values: VECTOR, each element must be STRING VECTOR with size >= 1
    if (!args[2]->isVector())
        throw IllegalArgumentException(__FUNCTION__, "[Plugin::RedisCluster] values must be a nested string vector.");

    VectorSP keyVec = args[1];
    VectorSP outer  = args[2];

    const int n = keyVec->size();
    if (outer->size() != n)
        throw IllegalArgumentException(__FUNCTION__, "[Plugin::RedisCluster] keys and values must have the same length.");

    // defaults; batchWin from policy; rightPush default true; threads default 1
    rightPush  = true;
    batchWin   = 0;   // 0 means use policy default; we¡¯ll set later
    numThreads = 1;

    if (args.size() >= 4) {
        if (args[3]->getForm() != ddb::DF_SCALAR || (args[3]->getType() != ddb::DT_BOOL && args[3]->getType() != ddb::DT_INT))
            throw IllegalArgumentException(__FUNCTION__, "[Plugin::RedisCluster] rightPush must be BOOL.");
        rightPush = args[3]->getBool();
    }
    if (args.size() >= 5) {
        if (!args[4]->isScalar() || args[4]->getType() != ddb::DT_INT)
            throw IllegalArgumentException(__FUNCTION__, "[Plugin::RedisCluster] batchWin must be INT scalar.");
        auto bw = args[4]->getInt();
        if (bw <= 0) throw IllegalArgumentException(__FUNCTION__, "[Plugin::RedisCluster] batchWin must be > 0.");
        batchWin = static_cast<std::size_t>(bw);
    }
    if (args.size() >= 6) {
        if (!args[5]->isScalar() || args[5]->getType() != ddb::DT_INT)
            throw IllegalArgumentException(__FUNCTION__, "[Plugin::RedisCluster] numThreads must be INT scalar.");
        numThreads = std::max(1, args[5]->getInt());
    }

    // materialize keys and validate inner vectors
    keys.reserve(n);
    vals.reserve(n);
    for (int i = 0; i < n; ++i) {
        keys.emplace_back(keyVec->getString(i));

        ConstantSP inner = outer->get(i);
        if (!inner->isVector() || inner->getType() != ddb::DT_STRING)
            throw IllegalArgumentException(__FUNCTION__, "[Plugin::RedisCluster] each element of values must be STRING vector.");
        VectorSP sv = inner;
        if (sv->size() < 1)
            throw IllegalArgumentException(__FUNCTION__, "[Plugin::RedisCluster] values vector must contain at least 1 element.");
        vals.emplace_back(sv);
    }
}

} // anonymous

ddb::ConstantSP ddb_rc_batchPush(ddb::Heap*, const std::vector<ConstantSP>& args)
{
    auto conn = rc::getConn(args[0]);
    rc::ConnFacade cf(*conn);

    std::vector<std::string>      keys;
    std::vector<ddb::VectorSP>    vals;
    bool                          rightPush;
    std::size_t                   batchWin;
    int                           numThreads;

    parse_args(args, keys, vals, rightPush, batchWin, numThreads);

    // Configure pipeline batch window if user provided.
    if (batchWin > 0) cf.setBatchWindow(batchWin);

    rc::ListService svc(cf);
    try {
        svc.batchPush(keys, vals, rightPush, numThreads);
    } catch (const std::exception& e) {
        throw RuntimeException(std::string("[Plugin::RedisCluster] batchPush failed: ") + e.what());
    }
    return new ddb::Void();
}

