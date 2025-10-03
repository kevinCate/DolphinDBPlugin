#include "plugin.h"
#include "ddbplugin/Plugin.h"
#include "ddbplugin/PluginLogger.h"
#include "rc_connection.h"
#include "commands/kv.h"
#include "core/Globals.h"
#include "commands/hash_batch.h"

// 资源名 & 全局句柄表
//static const std::string HANDLE_NAME = "redis cluster";
//static ddb::BackgroundResourceMap<RedisClusterConn> g_map("[Plugin::RedisCluster] ", HANDLE_NAME);

// onClose：当 DolphinDB 回收资源时调用
static void onClose(ddb::Heap*, std::vector<ddb::ConstantSP>&){ /* no-op */ }

ddb::ConstantSP redisClusterConnect(ddb::Heap* heap, const std::vector<ddb::ConstantSP>& args){
    if (args.size() < 2
        || !args[0]->isScalar() || args[0]->getType()!=ddb::DT_STRING
        || !args[1]->isScalar() || args[1]->getType()!=ddb::DT_INT) {
        throw ddb::IllegalArgumentException(__FUNCTION__, "[Plugin::RedisCluster] Usage: redisClusterConnect(host:string, port:int, [password:string], [poolSize:int], [readReplica:bool])"); // NOLINT(cert-err60-cpp)
    }

    sw::redis::ConnectionOptions conn_opt;
    conn_opt.host = args[0]->getString();
    conn_opt.port = args[1]->getInt();
    if (args.size() >= 3 && args[2]->isScalar() && args[2]->getType()==ddb::DT_STRING)
        conn_opt.password = args[2]->getString();  // 支持 ACL 时也可设置 conn_opt.user

    sw::redis::ConnectionPoolOptions pool_opt;
    pool_opt.wait_timeout = std::chrono::milliseconds(100);
    if (args.size() >= 4 && args[3]->isScalar()) pool_opt.size = std::max(1, args[3]->getInt());

    // 可选：读副本
    auto role = sw::redis::Role::MASTER;
    if (args.size() >= 5 && args[4]->isScalar() && args[4]->getBool()) {
        role = sw::redis::Role::SLAVE;  // 只允许只读命令
    }

    try {
        std::unique_ptr<sw::redis::RedisCluster> cluster;
        if (pool_opt.size > 1 || role==sw::redis::Role::SLAVE)
            cluster = std::make_unique<sw::redis::RedisCluster>(conn_opt, pool_opt, role);
        else
            cluster = std::make_unique<sw::redis::RedisCluster>(conn_opt);

        ddb::SmartPointer<rc::RedisClusterConn> conn = new rc::RedisClusterConn(std::move(cluster));
        ddb::FunctionDefSP onCloseProc(ddb::Util::createSystemProcedure("redis cluster onClose()", onClose, 1, 1));
        ddb::ConstantSP handle = ddb::Util::createResource(reinterpret_cast<long long>(conn.get()), rc::RC_HANDLE_NAME, onCloseProc, heap->currentSession());
        rc::g_rc_map.safeAdd(handle, conn, std::to_string(reinterpret_cast<long long>(conn.get())));
        return handle;
    } catch (const std::exception& e) {
        throw ddb::RuntimeException(std::string("RedisCluster connect failed: ") + e.what()); // NOLINT(cert-err60-cpp)
    }
}

ddb::ConstantSP redisClusterClose(ddb::Heap*, const std::vector<ddb::ConstantSP>& args){
    if (args.empty() || args[0]->getType()!=ddb::DT_RESOURCE || args[0]->getString()!=rc::RC_HANDLE_NAME)
        throw ddb::IllegalArgumentException(__FUNCTION__, "First argument must be a redis cluster handle."); // NOLINT(cert-err60-cpp)
    rc::g_rc_map.safeRemove(args[0]);
    return new ddb::String("ok");
}

// gGet(handle, key:string) -> string | NULL
ddb::ConstantSP redisClusterGet(ddb::Heap* h, const std::vector<ddb::ConstantSP>& args){ return ddb_rc_get(h, args); }

// set(handle, key:string, val:string, [ttlSec:int]) -> string("OK")
ddb::ConstantSP redisClusterSet(ddb::Heap* h, const std::vector<ddb::ConstantSP>& args){ return ddb_rc_set(h,args); }

// mget(handle, keys:stringVector) -> stringVector(可含 NULL)
ddb::ConstantSP redisClusterMget(ddb::Heap* h, const std::vector<ddb::ConstantSP>& args){ return ddb_rc_mget(h, args); }

ddb::ConstantSP redisClusterBatchHashSet(ddb::Heap* h, const std::vector<ddb::ConstantSP>& args){ return ddb_rc_batchHashSet(h, args); }

ddb::ConstantSP redisClusterBatchHashSetThread(ddb::Heap* h, const std::vector<ddb::ConstantSP>& args){ return ddb_rc_batchHashSetThread(h, args); }

ddb::ConstantSP redisClusterBatchDel(ddb::Heap* h, const std::vector<ddb::ConstantSP>& args){ return ddb_rc_deleteKeys(h, args); }
