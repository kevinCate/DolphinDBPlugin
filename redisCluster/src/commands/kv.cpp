//
// Created by uplee on 9/25/25.
//

#include "commands/kv.h"
#include "core/Globals.h"
#include "core/ConnFacade.h"
#include "services/KVService.h"

using rc::KVService;
using ddb::VectorSP;

namespace {

std::vector<std::string> toStdStringVec(const VectorSP& sv) {
    const auto n = sv->size();
    std::vector<std::string> out;
    out.reserve(n);
    for (int i = 0; i < n; ++i) out.emplace_back(sv->getString(i));
    return out;
}

} // anonymous

ddb::ConstantSP ddb_rc_get(ddb::Heap*, const std::vector<ddb::ConstantSP>& args){
    if (args.size()<2 || !args[1]->isScalar() || args[1]->getType()!=ddb::DT_STRING)
        throw ddb::IllegalArgumentException(__FUNCTION__, "Usage: get(handle, key:string)");

    auto conn = rc::getConn(args[0]);
    rc::ConnFacade cf(*conn);
    const KVService svc(cf);
    try{
        if (auto val = svc.get(args[1]->getString())) return new ddb::String(*val);
        return new ddb::Void();
    }catch(const std::exception& e){
        throw ddb::RuntimeException(std::string("GET failed: ")+e.what());
    }
}

ddb::ConstantSP ddb_rc_set(ddb::Heap*, const std::vector<ddb::ConstantSP>& args){
    if (args.size()<3
        || !args[1]->isScalar() || args[1]->getType()!=ddb::DT_STRING
        || !args[2]->isScalar() || args[2]->getType()!=ddb::DT_STRING)
        throw ddb::IllegalArgumentException(__FUNCTION__, "Usage: set(handle, key, val, [ttl:int])");

    auto conn = rc::getConn(args[0]);
    rc::ConnFacade cf(*conn);
    const KVService svc(cf);
    const auto& key = args[1]->getString();
    const auto& val = args[2]->getString();

    try{
        if (args.size()>=4 && args[3]->isScalar() && args[3]->getType()==ddb::DT_INT){
            int ttl = args[3]->getInt();
            if (ttl>0){ svc.setex(key, val, ttl); return new ddb::String("OK"); }
        }
        svc.set(key, val);
        return new ddb::String("OK");
    }catch(const std::exception& e){
        throw ddb::RuntimeException(std::string("SET failed: ")+e.what());
    }
}

ddb::ConstantSP ddb_rc_batchSet(ddb::Heap*, const std::vector<ddb::ConstantSP>& args){
    if (args.size()<3)
        throw ddb::IllegalArgumentException(__FUNCTION__, "Usage: batchSet(handle, keys, values, [batchWin:int=2048], [numThreads:int=3])");

    auto conn = rc::getConn(args[0]);
    rc::ConnFacade cf(*conn);
    const KVService svc(cf);

    // scalar-scalar fast path, mirrors standalone plugin
    if (args[1]->isScalar() && args[2]->isScalar()
        && args[1]->getType()==ddb::DT_STRING && args[2]->getType()==ddb::DT_STRING){
        try{
            svc.set(args[1]->getString(), args[2]->getString());
            return new ddb::String("OK");
        }catch(const std::exception& e){
            throw ddb::RuntimeException(std::string("SET failed: ")+e.what());
        }
    }

    // vector-vector path
    if (args[1]->getForm()!=ddb::DF_VECTOR || args[1]->getType()!=ddb::DT_STRING)
        throw ddb::IllegalArgumentException(__FUNCTION__, "Argument keys must be a STRING VECTOR when not scalar-scalar.");
    if (args[2]->getForm()!=ddb::DF_VECTOR || args[2]->getType()!=ddb::DT_STRING)
        throw ddb::IllegalArgumentException(__FUNCTION__, "Argument values must be a STRING VECTOR when not scalar-scalar.");

    ddb::VectorSP vkeys = args[1];
    ddb::VectorSP vvals = args[2];
    if (vkeys->size()!=vvals->size())
        throw ddb::IllegalArgumentException(__FUNCTION__, "keys and values must have the same size.");

    // optional tuning
    std::size_t batchWin = cf.policy().batch_window;
    int numThreads = 3;
    if (args.size()>=4){
        auto &bw=args[3];
        if (!bw->isScalar() || bw->getType()!=ddb::DT_INT)
            throw ddb::IllegalArgumentException(__FUNCTION__, "batchWin must be INT SCALAR if provided.");
        batchWin = static_cast<std::size_t>(bw->getInt());
        if (batchWin==0) throw ddb::IllegalArgumentException(__FUNCTION__, "batchWin must be > 0.");
    }
    if (args.size()>=5){
        auto &nt=args[4];
        if (!nt->isScalar() || nt->getType()!=ddb::DT_INT)
            throw ddb::IllegalArgumentException(__FUNCTION__, "numThreads must be INT SCALAR if provided.");
        numThreads = nt->getInt();
        if (numThreads<1) numThreads=1;
    }
    cf.setBatchWindow(batchWin);

    // materialize user data
    std::vector<std::string> keys = toStdStringVec(vkeys);
    std::vector<std::string> vals = toStdStringVec(vvals);

    try{
        svc.batchSet(std::move(keys), std::move(vals), numThreads);
        return new ddb::String("batchSet finish.");
    }catch(const std::exception& e){
        throw ddb::RuntimeException(std::string("batchSet failed: ")+e.what());
    }
}

