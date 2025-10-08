#include "plugin.h"
#include "rc_connection.h"
#include "commands/kv.h"
#include "core/Globals.h"
#include "commands/batch.h"
#include "commands/run.h"
#include "commands/list.h"

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

        const std::string address = conn_opt.host + ":" + std::to_string(conn_opt.port);
        ddb::SmartPointer<rc::RedisClusterConn> conn = new rc::RedisClusterConn(std::move(cluster), address);
        ddb::FunctionDefSP onCloseProc(ddb::Util::createSystemProcedure("redis cluster onClose()", onClose, 1, 1));
        ddb::ConstantSP handle = ddb::Util::createResource(reinterpret_cast<long long>(conn.get()), rc::RC_HANDLE_NAME, onCloseProc, heap->currentSession());

        const std::string token = std::to_string(reinterpret_cast<long long>(conn.get()));
        rc::g_rc_map.safeAdd(handle, conn, token);
        {
            std::lock_guard lk(rc::g_token_mu);
            rc::g_token2handle[token] = handle;
        }
        return handle;
    } catch (const std::exception& e) {
        throw ddb::RuntimeException(std::string("RedisCluster connect failed: ") + e.what()); // NOLINT(cert-err60-cpp)
    }
}

ddb::ConstantSP redisClusterRelease(ddb::Heap*, const std::vector<ddb::ConstantSP>& args){
    if (args.empty() || args[0]->getType()!=ddb::DT_RESOURCE || args[0]->getString()!=rc::RC_HANDLE_NAME)
        throw ddb::IllegalArgumentException(__FUNCTION__, "First argument must be a redis cluster handle."); // NOLINT(cert-err60-cpp)
    // compute token before removing
    auto sp = rc::getConn(args[0]);
    const std::string token = std::to_string(reinterpret_cast<long long>(sp.get()));
    {
        std::lock_guard lk(rc::g_token_mu);
        rc::g_token2handle.erase(token);
    }
    rc::g_rc_map.safeRemove(args[0]);
    return new ddb::String("ok");
}

ddb::ConstantSP redisClusterReleaseAll(ddb::Heap*, const std::vector<ddb::ConstantSP>& args){
    if (!args.empty())
        throw ddb::IllegalArgumentException(__FUNCTION__, "[Plugin::RedisCluster] Usage: releaseAll()");
    rc::g_rc_map.clear();
    return new ddb::String("ok");
}

ddb::ConstantSP redisClusterGetHandle(ddb::Heap*, const std::vector<ddb::ConstantSP>& args){
    if (args.size() != 1 || args[0]->getType()!=ddb::DT_STRING || args[0]->isVector())
        throw ddb::IllegalArgumentException(__FUNCTION__, "[Plugin::RedisCluster] Usage: getHandle(token: STRING)");

    const std::string token = args[0]->getString();
    ddb::ConstantSP handle;
    {
        std::lock_guard lk(rc::g_token_mu);
        auto it = rc::g_token2handle.find(token);
        if (it == rc::g_token2handle.end())
            throw ddb::IllegalArgumentException(__FUNCTION__, "[Plugin::RedisCluster] token not found.");
        handle = it->second;
    }
    // validate handle still live
    auto sp = rc::g_rc_map.safeGet(handle);
    if (sp.isNull()) {
        std::lock_guard lk(rc::g_token_mu);
        rc::g_token2handle.erase(token);
        throw ddb::RuntimeException("[Plugin::RedisCluster] invalid/expired handle for token.");
    }
    return handle;
}

ddb::ConstantSP redisClusterGetHandleStatus(ddb::Heap*, const std::vector<ddb::ConstantSP>& args){
    if (!args.empty())
        throw ddb::IllegalArgumentException(__FUNCTION__, "[Plugin::RedisCluster] Usage: getHandleStatus()");

    std::vector<std::string> tokens;
    std::vector<std::string> addrs;
    std::vector<ddb::DateTime> times;

    {
        std::lock_guard lk(rc::g_token_mu);
        for (auto it = rc::g_token2handle.begin(); it != rc::g_token2handle.end(); ) {
            const std::string& token = it->first;
            const ddb::ConstantSP& h = it->second;
            auto sp = rc::g_rc_map.safeGet(h);
            if (sp.isNull()) {
                it = rc::g_token2handle.erase(it); // clean stale
                continue;
            }
            tokens.push_back(token);
            addrs.push_back(sp->getAddress());
            times.push_back(sp->getCreatedTime());
            ++it;
        }
    }

    const int n = static_cast<int>(tokens.size());
    ddb::VectorSP colToken = ddb::Util::createVector(ddb::DT_STRING, n, n);
    ddb::VectorSP colAddr  = ddb::Util::createVector(ddb::DT_STRING, n, n);
    ddb::VectorSP colTime  = ddb::Util::createVector(ddb::DT_DATETIME, n, n);

    for (int i = 0; i < n; ++i) {
        colToken->set(i, new ddb::String(tokens[i]));
        colAddr->set(i,  new ddb::String(addrs[i]));
        colTime->set(i,  new ddb::DateTime(times[i]));
    }

    std::vector<ddb::ConstantSP> columns;
    columns.emplace_back(colToken);
    columns.emplace_back(colAddr);
    columns.emplace_back(colTime);

    std::vector<std::string> names = {"token","address","createdTime"};
    return ddb::Util::createTable(names, columns);
}

// gGet(handle, key:string) -> string | NULL
ddb::ConstantSP redisClusterGet(ddb::Heap* h, const std::vector<ddb::ConstantSP>& args){ return ddb_rc_get(h, args); }

// set(handle, key:string, val:string, [ttlSec:int]) -> string("OK")
ddb::ConstantSP redisClusterSet(ddb::Heap* h, const std::vector<ddb::ConstantSP>& args){ return ddb_rc_set(h,args); }

// mget(handle, keys:stringVector) -> stringVector(可含 NULL)
ddb::ConstantSP redisClusterBatchGet(ddb::Heap* h, const std::vector<ddb::ConstantSP>& args){ return ddb_rc_batchGet(h, args); }

ddb::ConstantSP redisClusterBatchHashSet(ddb::Heap* h, const std::vector<ddb::ConstantSP>& args){ return ddb_rc_batchHashSet(h, args); }

ddb::ConstantSP redisClusterBatchDel(ddb::Heap* h, const std::vector<ddb::ConstantSP>& args){ return ddb_rc_deleteKeys(h, args); }

ddb::ConstantSP redisClusterRun(ddb::Heap* h, const std::vector<ddb::ConstantSP>& args){ return ddb_rc_run(h, args); }

ddb::ConstantSP redisClusterBatchSet(ddb::Heap* h, const std::vector<ddb::ConstantSP>& args){ return ddb_rc_batchSet(h, args); }

ddb::ConstantSP redisClusterBatchPush(ddb::Heap* h, const std::vector<ddb::ConstantSP>& args){ return ddb_rc_batchPush(h, args); }