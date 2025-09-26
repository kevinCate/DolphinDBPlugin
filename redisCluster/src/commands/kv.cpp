//
// Created by uplee on 9/25/25.
//

#include "commands/kv.h"
#include "rc_connection.h"
#include "core/ClusterClient.h"
#include "core/Globals.h"

ddb::ConstantSP ddb_rc_get(ddb::Heap*, const std::vector<ddb::ConstantSP>& args){
    if (args.size()<2 || !args[1]->isScalar() || args[1]->getType()!=ddb::DT_STRING)
        throw ddb::IllegalArgumentException(__FUNCTION__, "Usage: get(handle, key:string)");

    auto conn = getConn(args[0]);
    ClusterClient cli(*conn);
    try{
        auto val = cli.get(args[1]->getString());
        if (val) return new ddb::String(*val);
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

    auto conn = getConn(args[0]);
    ClusterClient cli(*conn);
    const auto& key = args[1]->getString();
    const auto& val = args[2]->getString();

    try{
        if (args.size()>=4 && args[3]->isScalar() && args[3]->getType()==ddb::DT_INT){
            int ttl = args[3]->getInt();
            if (ttl>0){ cli.setex(key, val, ttl); return new ddb::String("OK"); }
        }
        cli.set(key, val);
        return new ddb::String("OK");
    }catch(const std::exception& e){
        throw ddb::RuntimeException(std::string("SET failed: ")+e.what());
    }
}

ddb::ConstantSP ddb_rc_mget(ddb::Heap*, const std::vector<ddb::ConstantSP>& args){
    if (args.size()<2 || !args[1]->isVector() || args[1]->getType()!=ddb::DT_STRING)
        throw ddb::IllegalArgumentException(__FUNCTION__, "Usage: mget(handle, keys:STRING VECTOR)");

    auto conn = getConn(args[0]);
    ClusterClient cli(*conn);

    ddb::VectorSP in  = ddb::VectorSP(args[1]);
    const int n  = in->size();
    std::vector<std::string> keys; keys.reserve(n);
    for (int i=0;i<n;++i) keys.emplace_back(in->getString(i));

    std::vector<sw::redis::OptionalString> vals;
    try{
        cli.mget(keys, vals);
    }catch(const std::exception& e){
        throw ddb::RuntimeException(std::string("MGET failed: ")+e.what());
    }

    ddb::VectorSP out = ddb::Util::createVector(ddb::DT_STRING, n);
    for (int i=0;i<n;++i){
        if (vals[i]) out->setString(i, *vals[i]);
        else         out->setNull(i);
    }
    return out;
}