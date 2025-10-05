//
// Created by uplee on 9/25/25.
//

#include "hash_batch.h"
#include "core/Globals.h"
#include "services/HashService.h"
#include "core/ConnFacade.h"
#include <string>
#include <vector>

using ddb::ConstantSP;
using ddb::VectorSP;
using ddb::TableSP;
using rc::HashService;

namespace {

std::vector<std::string> toStdStringVec(const VectorSP& sv) {
    const auto n = sv->size();
    std::vector<std::string> out;
    out.reserve(n);
    for (int i = 0; i < n; ++i) out.emplace_back(sv->getString(i));
    return out;
}

} // anonymous

ddb::ConstantSP ddb_rc_mget(ddb::Heap*, const std::vector<ddb::ConstantSP>& args) {
    if (args.size() < 2
        || !args[1]->isVector()
        || args[1]->getType() != ddb::DT_STRING)
    {
        throw ddb::IllegalArgumentException(__FUNCTION__,
            "Usage: mget(handle, keys:STRING VECTOR)");
    }

    auto conn = rc::getConn(args[0]);
    rc::ConnFacade cf(*conn);
    HashService svc(cf);

    VectorSP in = args[1];
    const int n = in->size();
    std::vector<std::string> keys;
    keys.reserve(n);
    for (int i = 0; i < n; ++i) keys.emplace_back(in->getString(i));

    std::vector<sw::redis::OptionalString> vals;
    try {
        svc.mget(keys, vals);
    } catch (const std::exception& e) {
        throw ddb::RuntimeException(std::string("MGET failed: ") + e.what());
    }

    VectorSP out = ddb::Util::createVector(ddb::DT_STRING, n);
    for (int i = 0; i < n; ++i) {
        if (vals[i]) out->setString(i, *vals[i]);
        else         out->setNull(i);
    }
    return out;
}

ddb::ConstantSP ddb_rc_batchHashSet(ddb::Heap*, const std::vector<ddb::ConstantSP>& args) {
    // args[0] = handle
    // args[1] = STRING vector (keys)
    // args[2] = TABLE (all STRING)
    // optional args[3] = batchWin (INT SCALAR)
    // optional args[4] = numThreads (INT SCALAR)

    if (args.size() < 3)
        throw ddb::IllegalArgumentException(__FUNCTION__,
            "Usage: batchHashSet(conn, ids:STRING VECTOR, fieldData:STRING TABLE, batchWin:INT SCALAR=default, numThreads:INT=1)");

    auto conn = rc::getConn(args[0]);
    if (args[1]->getForm() != ddb::DF_VECTOR || args[1]->getType() != ddb::DT_STRING) {
        throw ddb::IllegalArgumentException(__FUNCTION__,
            "Argument ids must be STRING VECTOR.");
    }
    TableSP tb = TableSP(args[2]);
    if (!tb || tb->getTableType() != ddb::BASICTBL) {
        throw ddb::IllegalArgumentException(__FUNCTION__,
            "Argument fieldData must be a BASIC TABLE.");
    }

    rc::ConnFacade cf(*conn);

    // batch window default from policy
    std::size_t batchWin = cf.policy().batch_window;
    int numThreads = 1;

    if (args.size() >= 4) {
        auto &bw = args[3];
        if (!bw->isScalar() || bw->getType() != ddb::DT_INT) {
            throw ddb::IllegalArgumentException(__FUNCTION__,
                "Argument batchWin must be INT SCALAR if provided.");
        }
        batchWin = static_cast<std::size_t>(bw->getInt());
        if (batchWin == 0)
            throw ddb::IllegalArgumentException(__FUNCTION__,
                "Argument batchWin must be > 0.");
    }

    if (args.size() >= 5) {
        auto &nt = args[4];
        if (!nt->isScalar() || nt->getType() != ddb::DT_INT) {
            throw ddb::IllegalArgumentException(__FUNCTION__,
                "Argument numThreads must be INT SCALAR if provided.");
        }
        numThreads = nt->getInt();
        if (numThreads < 1) numThreads = 1;  // fallback
    }

    const auto ids = toStdStringVec(args[1]);
    if (static_cast<std::size_t>(tb->size()) != ids.size()) {
        throw ddb::IllegalArgumentException(__FUNCTION__,
            "ids and fieldData must have the same number of rows.");
    }

    // Check all columns are string
    for (int c = 0; c < tb->columns(); ++c) {
        if (tb->getColumn(c)->getType() != ddb::DT_STRING) {
            throw ddb::RuntimeException("[Plugin::RedisCluster] fieldData columns must be STRING.");
        }
    }

    // Set policy
    cf.setBatchWindow(batchWin);
    // Optionally, enable new-connection policy or other flags if desired
    // cf.setNewConnection(true);

    HashService svc(cf);
    svc.batchHSet(ids, tb, numThreads);

    return new ddb::String("batchHashSet finish.");
}

ddb::ConstantSP ddb_rc_deleteKeys(ddb::Heap*, const std::vector<ddb::ConstantSP>& args) {
    if (args.size() < 2) {
        throw ddb::IllegalArgumentException(__FUNCTION__,
            "Usage: deleteKeys(conn, ids:STRING VECTOR, batchWin:INT=default, useUnlink:BOOL=true, numThreads:INT=1)");
    }

    auto conn = rc::getConn(args[0]);
    if (args[1]->getForm() != ddb::DF_VECTOR || args[1]->getType() != ddb::DT_STRING) {
        throw ddb::IllegalArgumentException(__FUNCTION__,
            "Argument ids must be STRING VECTOR.");
    }

    rc::ConnFacade cf(*conn);

    // default values
    std::size_t batchWin = cf.policy().batch_window;
    bool useUnlink = true;
    int numThreads = 1;

    if (args.size() >= 3) {
        auto &bw = args[2];
        if (!bw->isScalar() || bw->getType() != ddb::DT_INT) {
            throw ddb::IllegalArgumentException(__FUNCTION__,
                "Argument batchWin must be INT SCALAR if provided.");
        }
        batchWin = static_cast<std::size_t>(bw->getInt());
        if (batchWin == 0)
            throw ddb::IllegalArgumentException(__FUNCTION__,
                "Argument batchWin must be > 0.");
    }
    if (args.size() >= 4) {
        auto &nt = args[3];
        if (!nt->isScalar() || nt->getType() != ddb::DT_INT) {
            throw ddb::IllegalArgumentException(__FUNCTION__,
                "Argument numThreads must be INT SCALAR if provided.");
        }
        numThreads = nt->getInt();
        if (numThreads < 1) numThreads = 1;
    }
    if (args.size() >= 5) {
        auto &ul = args[4];
        if (!ul->isScalar() || (ul->getType() != ddb::DT_BOOL && ul->getType() != ddb::DT_INT)) {
            throw ddb::IllegalArgumentException(__FUNCTION__,
                "Argument useUnlink must be BOOL or INT.");
        }
        useUnlink = ul->getBool();
    }

    const auto ids = toStdStringVec(args[1]);

    cf.setBatchWindow(batchWin);

    HashService svc(cf);
    svc.deleteKeys(ids, useUnlink, numThreads);

    return new ddb::String("deleteKeys finish.");
}