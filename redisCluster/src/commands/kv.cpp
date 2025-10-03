//
// Created by uplee on 9/25/25.
//

#include "commands/kv.h"
#include "core/Globals.h"
#include "core/ConnFacade.h"
#include "services/KVService.h"

using rc::KVService;

ddb::ConstantSP ddb_rc_get(ddb::Heap*, const std::vector<ddb::ConstantSP>& args){
    if (args.size()<2 || !args[1]->isScalar() || args[1]->getType()!=ddb::DT_STRING)
        throw ddb::IllegalArgumentException(__FUNCTION__, "Usage: get(handle, key:string)");

    auto conn = rc::getConn(args[0]);
    rc::ConnFacade cf(*conn);
    KVService svc(cf);
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
    KVService svc(cf);
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
