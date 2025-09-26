//
// Created by uplee on 9/25/25.
//

#include "core/Globals.h"
#include "rc_connection.h"

const std::string RC_HANDLE_NAME = "redis cluster";
ddb::BackgroundResourceMap<RedisClusterConn> g_rc_map("[Plugin::RedisCluster] ", RC_HANDLE_NAME);
ddb::SmartPointer<RedisClusterConn> getConn(const ddb::ConstantSP& h){
    if (h->getType()!=ddb::DT_RESOURCE || h->getString()!=RC_HANDLE_NAME)
        throw ddb::IllegalArgumentException(__FUNCTION__, "[RedisCluster] first arg must be a redis cluster handle."); // NOLINT(cert-err60-cpp)
    auto sp = g_rc_map.safeGet(h);
    if (sp.isNull()) throw ddb::RuntimeException("[RedisCluster] invalid/expired handle."); // NOLINT(cert-err60-cpp)
    return sp;
}