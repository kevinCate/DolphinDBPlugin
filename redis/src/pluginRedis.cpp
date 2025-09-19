#include "pluginRedis.h"

#include "ddbplugin/Plugin.h"
#include "hiredis.h"
#include "redisConnection.h"

using ddb::DATA_TYPE;
using ddb::DF_SCALAR;
using ddb::DT_DATETIME;
using ddb::DT_INT;
using ddb::DT_RESOURCE;
using ddb::DT_STRING;
using ddb::FunctionDefSP;
using ddb::IllegalArgumentException;
using ddb::INDEX;
using ddb::RedisConnection;
using ddb::RuntimeException;
using ddb::SmartPointer;
using ddb::String;
using ddb::TableSP;
using ddb::Util;
using ddb::DT_LONG;
using ddb::DT_DOUBLE;
using ddb::DT_BOOL;
using ddb::DT_FLOAT;
using ddb::DT_NANOTIMESTAMP;

const string REDIS_CONNECTION_NAME = "redis connection";
const string REDIS_PREFIX = "[Plugin::Redis] BackgroundResourceMap: ";
const vector<string> REDIS_STATUS_COLUMN_NAMES = {"token", "address", "createdTime"};
const vector<DATA_TYPE> REDIS_STATUS_COLUMN_TYPES = {DT_STRING, DT_STRING, DT_DATETIME};

ddb::BackgroundResourceMap<RedisConnection> REDIS_HANDLE_MAP(REDIS_PREFIX, REDIS_CONNECTION_NAME);

static void doNothingOnClose(Heap *, vector<ConstantSP> &) {
    // do nothing
}

static void checkHandle(const ConstantSP &handle) {
    if (handle->getType() != DT_RESOURCE || handle->getString() != REDIS_CONNECTION_NAME) {
        throw IllegalArgumentException(__FUNCTION__, "[Plugin::Redis] First argument must be a redis handle.");
    }
}

static void checkHandleValid(const SmartPointer<RedisConnection> &handle) {
    if (handle.isNull()) {
        throw RuntimeException("[Plugin::Redis] Invalid redis handle.");
    }
}

ConstantSP redisPluginConnect(Heap *heap, const vector<ConstantSP> &args) {
    if (args[0]->getType() != DT_STRING || args[0]->getForm() != DF_SCALAR) {
        throw IllegalArgumentException(__FUNCTION__,
                                       "[Plugin::Redis] Usage: connect(host, port), host must be string type.");
    }
    if (args[1]->getType() != DT_INT || args[1]->getForm() != DF_SCALAR) {
        throw IllegalArgumentException(__FUNCTION__,
                                       "[Plugin::Redis] Usage: connect(host, port), port must be int type.");
    }

    redisContext *conn = redisConnect(args[0]->getString().c_str(), args[1]->getInt());
    if (!conn) {
        throw RuntimeException("[Plugin::Redis] Redis connection error: can't allocate redis context.");
    }
    if (conn->err) {
        string errMsg = "[Plugin::Redis] Redis connection error: " + string(conn->errstr);
        redisFree(conn);
        throw RuntimeException(errMsg);
    }

    SmartPointer<RedisConnection> redisHandler = new RedisConnection(conn, args[0]->getString(), args[1]->getInt());
    FunctionDefSP onClose(Util::createSystemProcedure("redis connection onClose()", doNothingOnClose, 1, 1));
    ConstantSP resource = Util::createResource(reinterpret_cast<long long>(redisHandler.get()), REDIS_CONNECTION_NAME,
                                               onClose, heap->currentSession());
    REDIS_HANDLE_MAP.safeAdd(resource, redisHandler, std::to_string(reinterpret_cast<long long>(redisHandler.get())));
    return resource;
}

ConstantSP redisPluginRun(Heap *, const vector<ConstantSP> &args) {
    checkHandle(args[0]);
    SmartPointer<RedisConnection> redisHandler = REDIS_HANDLE_MAP.safeGet(args[0]);
    checkHandleValid(redisHandler);
    return redisHandler->redisRun(args);
}

ConstantSP redisPluginBatchSet(Heap *, const vector<ConstantSP> &args) {
    checkHandle(args[0]);
    SmartPointer<RedisConnection> redisHandler = REDIS_HANDLE_MAP.safeGet(args[0]);
    checkHandleValid(redisHandler);

    if (args[1]->isScalar() && args[2]->isScalar()) {
        return redisHandler->redisRun({args[0], new String("SET"), args[1], args[2]}, "Set");
    }
    return redisHandler->redisBatchSet(args);
}

ConstantSP redisPluginBatchHashSet(Heap *, const vector<ConstantSP> &args) {
    checkHandle(args[0]);
    SmartPointer<RedisConnection> redisHandler = REDIS_HANDLE_MAP.safeGet(args[0]);
    checkHandleValid(redisHandler);
    return redisHandler->redisBatchHashSet(args);
}

ConstantSP redisPluginRelease(Heap *, const vector<ConstantSP> &args) {
    checkHandle(args[0]);
    REDIS_HANDLE_MAP.safeRemove(args[0]);
    return new String("release finish.");
}

ConstantSP redisPluginReleaseAll(Heap *, const vector<ConstantSP> &) {
    REDIS_HANDLE_MAP.clear();
    return new String("releaseAll finish.");
}

ConstantSP redisGetHandle(Heap *, const vector<ConstantSP> &args) {
    if (args[0]->getForm() != DF_SCALAR || args[0]->getType() != DT_STRING) {
        throw IllegalArgumentException(__FUNCTION__, "[Plugin::Redis] First argument must be a string scalar.");
    }
    return REDIS_HANDLE_MAP.getHandleByName(args[0]->getString());
}

ConstantSP redisGetHandleStaus(Heap *, const vector<ConstantSP> &) {
    const vector<string> &names = REDIS_HANDLE_MAP.getHandleNames();
    int num = names.size();
    TableSP statusTable = Util::createTable(REDIS_STATUS_COLUMN_NAMES, REDIS_STATUS_COLUMN_TYPES, 0, num);

    SmartPointer<RedisConnection> redisHandler;
    vector<ConstantSP> row(3);
    INDEX insertedRows;
    string errMsg;
    for (int i = 0; i < num; ++i) {
        redisHandler = REDIS_HANDLE_MAP.safeGetByName(names[i]);
        checkHandleValid(redisHandler);

        row[0] = new String(std::to_string(reinterpret_cast<long long>(redisHandler.get())));
        row[1] = new String(redisHandler->getAddress());
        row[2] = redisHandler->getCreatedTime().getValue();
        if (!statusTable->append(row, insertedRows, errMsg)) {
            throw RuntimeException("[Plugin::Redis] getStatus failed: " + errMsg);
        }
    }

    return statusTable;
}

ConstantSP redisBatchPush(Heap *, const vector<ConstantSP> &args) {
    checkHandle(args[0]);
    SmartPointer<RedisConnection> redisHandler = REDIS_HANDLE_MAP.safeGet(args[0]);
    checkHandleValid(redisHandler);
    return redisHandler->redisBatchPush(args);
}

ConstantSP redisBatchGet(Heap *, const vector<ConstantSP> &args) {
    checkHandle(args[0]);
    SmartPointer<RedisConnection> redisHandler = REDIS_HANDLE_MAP.safeGet(args[0]);
    checkHandleValid(redisHandler);
    return redisHandler->redisBatchGet(args);
}

ConstantSP redisPluginBatchSetPipe(Heap *, const vector<ConstantSP> &args) {
    checkHandle(args[0]);
    SmartPointer<RedisConnection> redisHandler = REDIS_HANDLE_MAP.safeGet(args[0]);
    checkHandleValid(redisHandler);

    if (args[1]->isScalar() && args[2]->isScalar()) {
        return redisHandler->redisRun({args[0], new String("SET"), args[1], args[2]}, "Set");
    }
    return redisHandler->redisBatchSetPipe(args);
}

ConstantSP redisPluginTsMAdd(Heap *, const vector<ConstantSP> &args) {
    checkHandle(args[0]);
    SmartPointer<RedisConnection> h = REDIS_HANDLE_MAP.safeGet(args[0]);
    checkHandleValid(h);

    if (args.size() < 4)
        throw IllegalArgumentException(__FUNCTION__, "[Plugin::Redis] Usage: tsMAdd(conn, keys, timestamps, values [,batchSize] [,sameSlot]).");

    // keys
    if (!(args[1]->isVector() && args[1]->getType() == DT_STRING))
        throw IllegalArgumentException(__FUNCTION__, "[Plugin::Redis] keys must be STRING vector.");

    // timestamps
    if (!(args[2]->isVector() && (args[2]->getType() == DT_LONG || args[2]->getType() == DT_NANOTIMESTAMP)))
        throw IllegalArgumentException(__FUNCTION__, "[Plugin::Redis] timestamps must be LONG (ms since epoch).");

    // values
    if (!(args[3]->isVector() && (args[3]->getType() == DT_DOUBLE || args[3]->getType() == DT_FLOAT)))
        throw IllegalArgumentException(__FUNCTION__, "[Plugin::Redis] values must be DOUBLE vector.");

    if (args[1]->size() != args[2]->size() || args[1]->size() != args[3]->size())
        throw IllegalArgumentException(__FUNCTION__, "[Plugin::Redis] keys, timestamps, values must have the same length.");

    // optional batchSize
    /*
    int batchSize = 1024;
    if (args.size() >= 5) {
        if (!(args[4]->isScalar() && args[4]->getType() == DT_INT))
            throw IllegalArgumentException(__FUNCTION__, "[Plugin::Redis] batchSize must be INT scalar.");
        batchSize = std::max(1, args[4]->getInt());
    }

    // optional sameSlot
    bool sameSlot = true;
    if (args.size() >= 6) {
        if (!(args[5]->isScalar() && args[5]->getType() == DT_BOOL))
            throw IllegalArgumentException(__FUNCTION__, "[Plugin::Redis] sameSlot must be BOOL scalar.");
        sameSlot = args[5]->getBool();
    }
    */
    int batchSize = 1024;
    return h->redisTsMAdd(args[1], args[2], args[3], batchSize);
}

ConstantSP redisPluginPing(Heap *, const vector<ConstantSP> &args) {
    checkHandle(args[0]);
    SmartPointer<RedisConnection> h = REDIS_HANDLE_MAP.safeGet(args[0]);
    checkHandleValid(h);
    // Reuse redisRun so we get uniform error handling & reply conversion
    return h->redisRun({args[0], new String("PING")}, "PING");
}