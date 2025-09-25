#include "plugin.h"
#include "ddbplugin/Plugin.h"
#include "ddbplugin/PluginLogger.h"
#include "redisClusterConnection.h"

using ddb::ConstantSP;
using ddb::String;
using ddb::FunctionDefSP;
using ddb::RuntimeException;
using ddb::IllegalArgumentException;
using ddb::Util;
using ddb::SmartPointer;

// 资源名 & 全局句柄表
static const std::string HANDLE_NAME = "redis cluster";
static ddb::BackgroundResourceMap<RedisClusterConn> g_map("[Plugin::RedisCluster] ", HANDLE_NAME);
// 取句柄 + 校验
static SmartPointer<RedisClusterConn> getConnFromHandle(const ConstantSP& h){
    if (h->getType()!=ddb::DT_RESOURCE || h->getString()!=HANDLE_NAME)
        throw IllegalArgumentException(__FUNCTION__, "[Plugin::RedisCluster] first arg must be a redis cluster handle."); // NOLINT(cert-err60-cpp)
    auto sp = g_map.safeGet(h);
    if (sp.isNull()) throw RuntimeException("[Plugin::RedisCluster] invalid/expired handle."); // NOLINT(cert-err60-cpp)
    return sp;
}

// onClose：当 DolphinDB 回收资源时调用
static void onClose(Heap*, std::vector<ConstantSP>&){ /* no-op */ }

ConstantSP redisClusterConnect(Heap* heap, const std::vector<ConstantSP>& args){
    if (args.size() < 2
        || !args[0]->isScalar() || args[0]->getType()!=ddb::DT_STRING
        || !args[1]->isScalar() || args[1]->getType()!=ddb::DT_INT) {
        throw IllegalArgumentException(__FUNCTION__, "[Plugin::RedisCluster] Usage: redisClusterConnect(host:string, port:int, [password:string], [poolSize:int], [readReplica:bool])"); // NOLINT(cert-err60-cpp)
    }

    sw::redis::ConnectionOptions conn_opt;
    conn_opt.host = args[0]->getString();
    conn_opt.port = args[1]->getInt();
    if (args.size() >= 3 && args[2]->isScalar() && args[2]->getType()==ddb::DT_STRING)
        conn_opt.password = args[2]->getString();  // 支持 ACL 时也可设置 conn_opt.user

    sw::redis::ConnectionPoolOptions pool_opt;
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

        SmartPointer<RedisClusterConn> conn = new RedisClusterConn(std::move(cluster), conn_opt.host + ":" + std::to_string(conn_opt.port));
        FunctionDefSP onCloseProc(Util::createSystemProcedure("redis cluster onClose()", onClose, 1, 1));
        ConstantSP handle = Util::createResource(reinterpret_cast<long long>(conn.get()), HANDLE_NAME, onCloseProc, heap->currentSession());
        g_map.safeAdd(handle, conn, std::to_string(reinterpret_cast<long long>(conn.get())));
        return handle;
    } catch (const std::exception& e) {
        throw RuntimeException(std::string("RedisCluster connect failed: ") + e.what()); // NOLINT(cert-err60-cpp)
    }
}

ConstantSP redisClusterClose(Heap*, const std::vector<ConstantSP>& args){
    if (args.empty() || args[0]->getType()!=ddb::DT_RESOURCE || args[0]->getString()!=HANDLE_NAME)
        throw IllegalArgumentException(__FUNCTION__, "First argument must be a redis cluster handle."); // NOLINT(cert-err60-cpp)
    g_map.safeRemove(args[0]);
    return new String("ok");
}

// gGet(handle, key:string) -> string | NULL
ConstantSP redisClusterGet(Heap*, const std::vector<ConstantSP>& args){
    if (args.size()<2 || !args[1]->isScalar() || args[1]->getType()!=ddb::DT_STRING)
        throw IllegalArgumentException(__FUNCTION__, "Usage: redisClusterGet(handle, key:string)"); // NOLINT(cert-err60-cpp)
    auto conn = getConnFromHandle(args[0]);

    try{
        auto opt = conn->get().get(args[1]->getString());
        if (opt) return new String(*opt);           // 命中
        return new ddb::Void();                     // 未命中 -> NULL
    }catch(const std::exception& e){
        throw RuntimeException(std::string("GET failed: ")+e.what()); // NOLINT(cert-err60-cpp)
    }
}

// set(handle, key:string, val:string, [ttlSec:int]) -> string("OK")
ConstantSP redisClusterSet(Heap*, const std::vector<ConstantSP>& args){
    if (args.size()<3
        || !args[1]->isScalar() || args[1]->getType()!=ddb::DT_STRING
        || !args[2]->isScalar() || args[2]->getType()!=ddb::DT_STRING)
        throw IllegalArgumentException(__FUNCTION__, "Usage: redisClusterSet(handle, key:string, val:string, [ttl:int])"); // NOLINT(cert-err60-cpp)

    auto conn = getConnFromHandle(args[0]);
    const std::string key = args[1]->getString();
    const std::string val = args[2]->getString();

    try{
        if (args.size()>=4 && args[3]->isScalar() && args[3]->getType()==ddb::DT_INT){
            const int ttl = args[3]->getInt();
            if (ttl>0){
                // EX 秒
                conn->get().setex(key, std::chrono::seconds(ttl), val);
                return new String("OK");
            }
        }
        conn->get().set(key, val);
        return new String("OK");
    }catch(const std::exception& e){
        throw RuntimeException(std::string("SET failed: ")+e.what()); // NOLINT(cert-err60-cpp)
    }
}

// mget(handle, keys:stringVector) -> stringVector(可含 NULL)
ConstantSP redisClusterMget(Heap*, const std::vector<ConstantSP>& args){
    if (args.size()<2 || !args[1]->isVector() || args[1]->getType()!=ddb::DT_STRING)
        throw IllegalArgumentException(__FUNCTION__, "Usage: redisClusterMGet(handle, keys:STRING VECTOR)"); // NOLINT(cert-err60-cpp)

    auto conn = getConnFromHandle(args[0]);

    // 输出：与输入等长的 STRING 向量；未命中位置置 NULL
    const auto inVec  = ddb::VectorSP(args[1]);
    const auto n      = inVec->size();
    ddb::VectorSP outVec = Util::createVector(ddb::DT_STRING, n);

    try{
        for (int i=0; i<n; ++i){
            std::string k = inVec->getString(i);
            auto opt = conn->get().get(k);
            if (opt) outVec->setString(i, *opt);
            else     outVec->setNull(i);
        }
        return outVec;
    }catch(const std::exception& e){
        throw RuntimeException(std::string("MGET (loop) failed: ")+e.what()); // NOLINT(cert-err60-cpp)
    }
}