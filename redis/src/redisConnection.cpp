#include "redisConnection.h"

#include <ctime>

static std::string d2str(double x){
    // 17 位足以 round-trip 双精度
    char buf[32];
    int n = std::snprintf(buf, sizeof(buf), "%.17g", x);
    return std::string(buf, n > 0 ? n : 0);
}

static long long toMillis(bool isNano, long long v){
    return isNano ? (v / 1000000LL) : v;
}

namespace ddb {

    const int BUFFER_SIZE = 1024;
    const string CONN_ERR = ", this connection cannot be reused and you should release it and create a new connection.";

    class RedisReplyGuard {
      public:
        RedisReplyGuard(redisReply *r) : reply_(r) {}

        ~RedisReplyGuard() {
            if (reply_ != nullptr) {
                freeReplyObject(reply_);
            }
        }

      private:
        redisReply *reply_ = nullptr;
    };

    static ConstantSP convertRedisReply(const redisReply *const reply) {
        if (reply == nullptr) {
            return new Void();
        }

        switch (reply->type) {
            case REDIS_REPLY_ERROR:
                throw RuntimeException("[Plugin::Redis] Redis reply error: " + string(reply->str));

            case REDIS_REPLY_STRING:
            case REDIS_REPLY_STATUS:
            case REDIS_REPLY_BIGNUM:
            case REDIS_REPLY_VERB:
                return new String(DolphinString(reply->str, reply->len));

            case REDIS_REPLY_NIL:
                return new Void();

            case REDIS_REPLY_INTEGER:
                return new Long(reply->integer);

            case REDIS_REPLY_DOUBLE:
                return new Double(reply->dval);

            case REDIS_REPLY_BOOL:
                return new Bool(reply->integer);

            case REDIS_REPLY_ARRAY: {
                size_t size = reply->elements;
                VectorSP vec = Util::createVector(DT_ANY, size);
                for (size_t i = 0; i < size; i++) {
                    vec->set(i, convertRedisReply(reply->element[i]));
                }
                return vec;
            }

            default:
                throw RuntimeException("[Plugin::Redis] Not support this redis reply type: " + std::to_string(reply->type) +
                                       ".");
        }
    }

    RedisConnection::RedisConnection(redisContext *redisConnection, const string &ip, const int &port)
        : redisConnect_(redisConnection) {
        address_ = ip + ":" + std::to_string(port);

        std::time_t t = std::time(0);
        std::tm *now = std::localtime(&t);
        if (now) {
            datetime_ =
                DateTime(1900 + now->tm_year, 1 + now->tm_mon, now->tm_mday, now->tm_hour, now->tm_min, now->tm_sec);
        }
    }

    RedisConnection::~RedisConnection() {
        if (redisConnect_) {
            redisFree(redisConnect_);
        }
    }

    ConstantSP RedisConnection::redisRun(const vector<ConstantSP> &args, const string& command) {
        size_t sz = args.size();
        for (size_t i = 1; i < sz; i++) {
            if (args[i]->getForm() != DF_SCALAR || args[i]->getType() != DT_STRING) {
                throw IllegalArgumentException(
                    __FUNCTION__, "[Plugin::Redis] argument " + std::to_string(i + 1) + " must be a string scalar.");
            }
        }
        LockGuard<Mutex> guard(&redisMutex_);

        int argsLen = sz - 1;
        vector<string> strings;
        strings.reserve(argsLen);
        for (size_t i = 1; i < sz; i++) {
            strings.emplace_back(args[i]->getString());
        }

        vector<size_t> argvlen;
        argvlen.reserve(argsLen);
        vector<const char *> argv;
        argv.reserve(argsLen);
        for (size_t i = 0; i < strings.size(); i++) {
            argv.push_back(strings[i].c_str());
            argvlen.push_back(strings[i].size());
        }

        redisReply *reply = static_cast<redisReply *>(redisCommandArgv(
            redisConnect_, argsLen, static_cast<const char **>(argv.data()), static_cast<const size_t *>(argvlen.data())));
        RedisReplyGuard replyGuard(reply);
        checkReply(reply, command);
        return convertRedisReply(reply);
    }

    // TODO: use pipeline (redisAppendCommandArgv) to optimize it
    ConstantSP RedisConnection::redisBatchSet(const vector<ConstantSP> &args) {
        if (args[1]->getForm() != DF_VECTOR || args[1]->getType() != DT_STRING) {
            throw IllegalArgumentException(__FUNCTION__, "[Plugin::Redis] Argument keys must be a string vector.");
        }
        if (args[2]->getForm() != DF_VECTOR || args[2]->getType() != DT_STRING) {
            throw IllegalArgumentException(__FUNCTION__, "[Plugin::Redis] Argument values must be a string vector.");
        }
        if (args[1]->size() != args[2]->size()) {
            throw IllegalArgumentException(__FUNCTION__,
                                           "[Plugin::Redis] Argument keys and values must have the same size.");
        }
        LockGuard<Mutex> guard(&redisMutex_);

        char *keysBuffer[BUFFER_SIZE];
        char *valuesBuffer[BUFFER_SIZE];
        int sz = args[1]->size();
        int rest = sz, nread = 0, read = 0;
        for (; rest > 0; nread += read, rest = sz - nread) {
            if (rest >= BUFFER_SIZE) {
                args[1]->getString(nread, BUFFER_SIZE, keysBuffer);
                args[2]->getString(nread, BUFFER_SIZE, valuesBuffer);
                read = BUFFER_SIZE;
            } else {
                args[1]->getString(nread, rest, keysBuffer);
                args[2]->getString(nread, rest, valuesBuffer);
                read = rest;
            }

            for (int i = 0; i < read; i++) {
                const char *setArgv[3] = {"SET", keysBuffer[i], valuesBuffer[i]};
                const size_t setArgvLen[3] = {3, strlen(keysBuffer[i]), strlen(valuesBuffer[i])};
                redisReply *reply = static_cast<redisReply *>(redisCommandArgv(redisConnect_, 3, setArgv, setArgvLen));
                RedisReplyGuard replyGuard(reply);
                checkReply(reply, "Set");
            }
        }
        return new String("batchSet finish.");
    }

    ConstantSP RedisConnection::redisBatchHashSet(const vector<ConstantSP> &args) {
        if (args[1]->getForm() != DF_VECTOR || args[1]->getType() != DT_STRING) {
            throw IllegalArgumentException(__FUNCTION__, "[Plugin::Redis] Argument idCol must be a string vector.");
        }
        if (!args[2]->isTable() || ((Table *)args[2].get())->getTableType() != BASICTBL) {
            throw IllegalArgumentException(__FUNCTION__, "[Plugin::Redis] Argument tb must be a basic table.");
        }
        if (args[1]->size() != args[2]->size()) {
            throw IllegalArgumentException(__FUNCTION__,
                                           "[Plugin::Redis] Arguments idCol and tb must have the same num of rows.");
        }
        LockGuard<Mutex> guard(&redisMutex_);

        DolphinString *idBuffer[BUFFER_SIZE];
        DolphinString *valueBuffer[BUFFER_SIZE];
        DolphinString *valueBuffer2[BUFFER_SIZE];
        DolphinString *valueBuffer3[BUFFER_SIZE];
        DolphinString *valueBuffer4[BUFFER_SIZE];
        int numCols = ((Table *)args[2].get())->columns();
        for (int colIndex = 0; colIndex < numCols;) {
            bool useBatch = numCols - colIndex >= 4;
            INDEX start = 0;
            INDEX len = args[1]->size();
            VectorSP idCol = args[1];
            while (start < len) {
                int count = std::min(len - start, BUFFER_SIZE);
                DolphinString **ids = idCol->getStringConst(start, count, idBuffer);

                if (useBatch) {
                    VectorSP col = ((Table *)args[2].get())->getColumn(colIndex);
                    VectorSP col2 = ((Table *)args[2].get())->getColumn(colIndex + 1);
                    VectorSP col3 = ((Table *)args[2].get())->getColumn(colIndex + 2);
                    VectorSP col4 = ((Table *)args[2].get())->getColumn(colIndex + 3);
                    if (col->getType() != DT_STRING || col2->getType() != DT_STRING || col3->getType() != DT_STRING ||
                        col4->getType() != DT_STRING) {
                        throw RuntimeException("[Plugin::Redis] The type of field column need to be string.");
                    }
                    const char *fieldName = ((Table *)args[2].get())->getColumnName(colIndex).c_str();
                    const char *fieldName2 = ((Table *)args[2].get())->getColumnName(colIndex + 1).c_str();
                    const char *fieldName3 = ((Table *)args[2].get())->getColumnName(colIndex + 2).c_str();
                    const char *fieldName4 = ((Table *)args[2].get())->getColumnName(colIndex + 3).c_str();
                    DolphinString **values = col->getStringConst(start, count, valueBuffer);
                    DolphinString **values2 = col2->getStringConst(start, count, valueBuffer2);
                    DolphinString **values3 = col3->getStringConst(start, count, valueBuffer3);
                    DolphinString **values4 = col4->getStringConst(start, count, valueBuffer4);
                    for (int i = 0; i < count; ++i) {
                        vector<const char *> argv(10);
                        vector<size_t> argvlen(10);
                        argv[0] = "HSET";
                        argvlen[0] = 4;
                        argv[1] = ids[i]->c_str();
                        argvlen[1] = strlen(argv[1]);

                        argv[2] = fieldName;
                        argvlen[2] = strlen(argv[2]);
                        argv[3] = values[i]->c_str();
                        argvlen[3] = strlen(argv[3]);

                        argv[4] = fieldName2;
                        argvlen[4] = strlen(argv[4]);
                        argv[5] = values2[i]->c_str();
                        argvlen[5] = strlen(argv[5]);

                        argv[6] = fieldName3;
                        argvlen[6] = strlen(argv[6]);
                        argv[7] = values3[i]->c_str();
                        argvlen[7] = strlen(argv[7]);

                        argv[8] = fieldName4;
                        argvlen[8] = strlen(argv[8]);
                        argv[9] = values4[i]->c_str();
                        argvlen[9] = strlen(argv[9]);
                        redisAppendCommandArgv(redisConnect_, 10, &argv[0], &argvlen[0]);
                    }
                } else {
                    VectorSP col = ((Table *)args[2].get())->getColumn(colIndex);
                    if (col->getType() != DT_STRING) {
                        throw RuntimeException("[Plugin::Redis] The type of field column need to be string.");
                    }
                    const char *fieldName = ((Table *)args[2].get())->getColumnName(colIndex).c_str();
                    DolphinString **values = col->getStringConst(start, count, valueBuffer);
                    for (int i = 0; i < count; ++i) {
                        vector<const char *> argv(4);  // ["HSET", id, field, value]
                        vector<size_t> argvlen(4);
                        argv[0] = "HSET";
                        argvlen[0] = 4;
                        argv[1] = ids[i]->c_str();
                        argvlen[1] = strlen(argv[1]);

                        argv[2] = fieldName;
                        argvlen[2] = strlen(argv[2]);
                        argv[3] = values[i]->c_str();
                        argvlen[3] = strlen(argv[3]);
                        redisAppendCommandArgv(redisConnect_, 4, &argv[0], &argvlen[0]);
                    }
                }

                for (int i = 0; i < count; ++i) {
                    redisReply *reply;
                    if (redisGetReply(redisConnect_, (void **)&reply) != REDIS_OK) {
                        string connMsg = (redisConnect_->err) ? CONN_ERR : ".";
                        throw RuntimeException(
                            "[Plugin::Redis] Failed to execute HSET command: " + string(redisConnect_->errstr) + connMsg);
                    }
                    RedisReplyGuard replyGuard(reply);
                    checkReply(reply, "HSET");
                }
                start += count;
            }
            colIndex = useBatch ? colIndex + 4 : colIndex + 1;
        }
        return new String("batchHashSet finish.");
    }

    static string checkArgsInBatchPush(const vector<ConstantSP> &args) {
        if (args[1]->getForm() != DF_VECTOR || args[1]->getType() != DT_STRING) {
            throw IllegalArgumentException(__FUNCTION__, "[Plugin::Redis] Argument keys must be a string vector.");
        }

        if (!args[2]->isVector()) {
            throw IllegalArgumentException(__FUNCTION__,
                                           "[Plugin::Redis] Argument values must be a two-level nested string vector.");
        }

        ConstantSP value;
        int size = args[2]->size();
        for (int i = 0; i < size; ++i) {
            value = args[2]->get(i);
            if (!value->isVector() || value->getType() != DT_STRING) {
                throw IllegalArgumentException(__FUNCTION__,
                                               "[Plugin::Redis] Argument values must be a two-level nested string vector.");
            }
            if (value->size() < 1) {
                throw IllegalArgumentException(__FUNCTION__,
                                               "[Plugin::Redis] The num of elements in values must greater than 0.");
            }
        }

        bool pushRight = true;
        if (args.size() > 3) {
            if (args[3]->getForm() != DF_SCALAR || args[3]->getType() != DT_BOOL) {
                throw IllegalArgumentException(__FUNCTION__, "[Plugin::Redis] Argument pushRight must be a bool scalar.");
            }
            pushRight = args[3]->getBool();
        }

        if (args[1]->size() != args[2]->size()) {
            throw IllegalArgumentException(__FUNCTION__,
                                           "[Plugin::Redis] Arguments keys and values must have the same num of elements.");
        }
        return pushRight ? "RPUSH" : "LPUSH";
    }

    ConstantSP RedisConnection::redisBatchPush(const vector<ConstantSP> &args) {
        string command = checkArgsInBatchPush(args);
        LockGuard<Mutex> guard(&redisMutex_);

        vector<const char *> argv(1);
        vector<size_t> argvlen(1);
        argv[0] = command.c_str();
        argvlen[0] = 5;

        DolphinString *keyBuffer[BUFFER_SIZE];
        DolphinString *valueBuffer[BUFFER_SIZE];

        INDEX len = args[1]->size();
        VectorSP keyVec = args[1];
        VectorSP outValueVec = args[2];
        INDEX start = 0;
        while (start < len) {
            int count = std::min(len - start, BUFFER_SIZE);
            DolphinString **keys = keyVec->getStringConst(start, count, keyBuffer);

            for (int i = 0; i < count; ++i) {
                VectorSP valueVec = outValueVec->get(start + i);
                int valueSize = valueVec->size();
                argv.resize(2 + valueSize);
                argvlen.resize(2 + valueSize);

                argv[1] = keys[i]->c_str();
                argvlen[1] = strlen(argv[1]);

                INDEX len2 = valueSize;
                INDEX start2 = 0;
                while (start2 < len2) {
                    int count2 = std::min(len2 - start2, BUFFER_SIZE);
                    DolphinString **values = valueVec->getStringConst(start2, count2, valueBuffer);

                    for (int j = 0; j < count2; ++j) {
                        argv[2 + start2 + j] = values[j]->c_str();
                        argvlen[2 + start2 + j] = strlen(argv[2 + start2 + j]);
                    }
                    start2 += count2;
                }
                redisAppendCommandArgv(redisConnect_, argv.size(), &argv[0], &argvlen[0]);
            }

            for (int i = 0; i < count; ++i) {
                redisReply *reply;
                if (redisGetReply(redisConnect_, (void **)&reply) != REDIS_OK) {
                    string connMsg = (redisConnect_->err) ? CONN_ERR : ".";
                    throw RuntimeException("[Plugin::Redis] Failed to execute " + command +
                                           " command: " + string(redisConnect_->errstr) + connMsg);
                }
                RedisReplyGuard replyGuard(reply);
                checkReply(reply, command);
            }
            start += count;
        }
        return new Void();
    }

    ConstantSP RedisConnection::redisBatchGet(const vector<ConstantSP> &args) {
        if (args[1]->getForm() != DF_VECTOR || args[1]->getType() != DT_STRING) {
            throw IllegalArgumentException(__FUNCTION__, "[Plugin::Redis] Argument keys must be a string vector.");
        }
        LockGuard<Mutex> guard(&redisMutex_);

        VectorSP keyVec = args[1];
        int len = keyVec->size();
        vector<string> valueVec(len);
        DolphinString *keyBuffer[BUFFER_SIZE];

        int start = 0;
        while (start < len) {
            int count = std::min(len - start, BUFFER_SIZE);
            vector<string> tmpStrs(count + 1);
            vector<const char *> argv(count + 1);
            vector<size_t> argvlen(count + 1);

            tmpStrs[0] = "MGET";
            argv[0] = tmpStrs[0].c_str();
            argvlen[0] = tmpStrs[0].length();
            DolphinString **keys = keyVec->getStringConst(start, count, keyBuffer);
            for (int i = 0; i < count; ++i) {
                tmpStrs[i + 1] = keys[i]->getString();
                argv[i + 1] = tmpStrs[i + 1].c_str();
                argvlen[i + 1] = tmpStrs[i + 1].length();
            }

            redisReply *reply =
                static_cast<redisReply *>(redisCommandArgv(redisConnect_, argv.size(), &argv[0], &argvlen[0]));
            RedisReplyGuard replyGuard(reply);
            checkReply(reply, "MGET");

            if (reply->type != REDIS_REPLY_ARRAY) {
                throw RuntimeException("[Plugin::Redis] The reply of mget is not array.");
            }
            if (reply->elements != static_cast<size_t>(count)) {
                throw RuntimeException("[Plugin::Redis] The num of mget reply values is not same with keys.");
            }
            for (size_t i = 0; i < reply->elements; i++) {
                redisReply *item = reply->element[i];
                if (item->type == REDIS_REPLY_STRING) {
                    valueVec[start + i] = string(item->str, item->len);
                }
            }
            start += count;
        }

        VectorSP result = Util::createVector(DT_STRING, len, len);
        result->setString(0, len, valueVec.data());
        return result;
    }

    void RedisConnection::checkReply(const redisReply *reply, const string &command) {
        if (redisConnect_->err) {
            throw RuntimeException("[Plugin::Redis] Execute command failed: " + string(redisConnect_->errstr) + CONN_ERR);
        }
        if (reply == nullptr) {
            throw RuntimeException("[Plugin::Redis] Invalid redis reply.");
        }
        if (reply->type == REDIS_REPLY_ERROR) {
            throw RuntimeException("[Plugin::Redis] " + command + " failed: " + string(reply->str));
        }
    }

    ConstantSP RedisConnection::redisBatchSetPipe(const vector<ConstantSP> &args) {
        // 1) 基本参数校验（与原版一致）
        if (args[1]->getForm() != DF_VECTOR || args[1]->getType() != DT_STRING) {
            throw IllegalArgumentException(__FUNCTION__, "[Plugin::Redis] Argument keys must be a string vector.");
        }
        if (args[2]->getForm() != DF_VECTOR || args[2]->getType() != DT_STRING) {
            throw IllegalArgumentException(__FUNCTION__, "[Plugin::Redis] Argument values must be a string vector.");
        }
        if (args[1]->size() != args[2]->size()) {
            throw IllegalArgumentException(__FUNCTION__,
                                           "[Plugin::Redis] Argument keys and values must have the same size.");
        }

        // 2) 线程安全
        LockGuard<Mutex> guard(&redisMutex_);

        // 3) 批处理缓冲：从 DolphinDB 向量分批提取 C 字符串指针
        char *keysBuffer[BUFFER_SIZE];
        char *valuesBuffer[BUFFER_SIZE];

        // pipeline 子批大小（可按实际网络/实例调优：256~4096）
        // 注意：这里的 PIPE_BATCH <= BUFFER_SIZE 通常更易于实现
        const int PIPE_BATCH = 1024;

        const int sz   = args[1]->size();
        int       rest = sz;
        int       nread = 0;  // 已处理总数
        int       read  = 0;  // 本次从 DolphinDB 拉取的个数

        for (; rest > 0; nread += read, rest = sz - nread) {
            // 3.1 拉取一段 keys/values 到本地缓冲
            if (rest >= BUFFER_SIZE) {
                args[1]->getString(nread, BUFFER_SIZE, keysBuffer);
                args[2]->getString(nread, BUFFER_SIZE, valuesBuffer);
                read = BUFFER_SIZE;
            } else {
                args[1]->getString(nread, rest, keysBuffer);
                args[2]->getString(nread, rest, valuesBuffer);
                read = rest;
            }

            // 4) 对这一段使用 pipeline：先 append 多条，再统一 getReply
            int sentInBatch = 0;

            for (int i = 0; i < read; ++i) {
                const char *argv[3]     = {"SET", keysBuffer[i], valuesBuffer[i]};
                const size_t argvlen[3] = {3, strlen(keysBuffer[i]), strlen(valuesBuffer[i])};

                // 4.1 追加命令到输出缓冲（binary-safe）
                // hiredis pipeline 的核心：append 不会立刻阻塞读取，命令按顺序缓存
                if (redisAppendCommandArgv(redisConnect_, 3, argv, argvlen) == REDIS_ERR) {
                    // 可进一步读取 redisConnect_->err / errstr 做更详细报错
                    throw RuntimeException("[Plugin::Redis] append failed in pipeline");
                }
                ++sentInBatch;

                // 4.2 到达子批上限：先把这部分回复全部 drain 掉
                if (sentInBatch == PIPE_BATCH) {
                    for (int k = 0; k < sentInBatch; ++k) {
                        redisReply *reply = nullptr;
                        if (redisGetReply(redisConnect_, (void**)&reply) == REDIS_ERR) {
                            throw RuntimeException("[Plugin::Redis] getReply failed in pipeline");
                        }
                        RedisReplyGuard replyGuard(reply);
                        checkReply(reply, "Set");
                    }
                    sentInBatch = 0;
                }
            }

            // 4.3 把本段最后不足一批的回复也 drain 掉
            if (sentInBatch > 0) {
                for (int k = 0; k < sentInBatch; ++k) {
                    redisReply *reply = nullptr;
                    if (redisGetReply(redisConnect_, (void**)&reply) == REDIS_ERR) {
                        throw RuntimeException("[Plugin::Redis] getReply failed in pipeline");
                    }
                    RedisReplyGuard replyGuard(reply);
                    checkReply(reply, "Set");
                }
            }
        }

        // 5) 完成
        return new String("batchSet finish (pipelined).");
    }

    ConstantSP RedisConnection::redisTsMAdd(const ConstantSP& keysVec,
                                            const ConstantSP& tsVec,
                                            const ConstantSP& valVec,
                                            int batchSize) {
        // 1) 基本校验与线程安全（对齐 batchSetPipe 风格）
        if (!(keysVec->isVector() && keysVec->getType()==DT_STRING))
            throw IllegalArgumentException(__FUNCTION__, "[Plugin::Redis] keys must be STRING vector.");
        if (!(tsVec->isVector() && (tsVec->getType()==DT_LONG || tsVec->getType()==DT_NANOTIMESTAMP)))
            throw IllegalArgumentException(__FUNCTION__, "[Plugin::Redis] timestamps must be LONG or NANOTIMESTAMP vector.");
        if (!(valVec->isVector() && (valVec->getType()==DT_DOUBLE || valVec->getType()==DT_FLOAT)))
            throw IllegalArgumentException(__FUNCTION__, "[Plugin::Redis] values must be DOUBLE/FLOAT vector.");
        if (keysVec->size()!=tsVec->size() || keysVec->size()!=valVec->size())
            throw IllegalArgumentException(__FUNCTION__, "[Plugin::Redis] keys/timestamps/values must have same length.");

        LockGuard<Mutex> guard(&redisMutex_);

        const INDEX N = keysVec->size();
        if (N==0) return new Void();

        // 2) 批参数（完全套用 batchSetPipe 的思路）
        // - 外层：从 DDB vector 里按 BUFFER_SIZE 拉一段到本地
        // - 内层：把这一段再切成多条 TS.MADD，每条携带 TUPLES_PER_CMD 个三元组
        // - 每凑满 PIPE_BATCH 条命令就 drain 一次（和 batchSetPipe 相同）
        const bool isNanoTs = (tsVec->getType()==DT_NANOTIMESTAMP);

        // “每条 MADD 装多少三元组”
        const int TUPLES_PER_CMD = std::max(1, batchSize);      // 你也可以限定到 1024/2048 做保守
        // “每次 drain 几条命令的回复”
        const int PIPE_BATCH_CMDS = 1024;                       // 对齐 batchSetPipe 中的 1024

        DolphinString* kb[BUFFER_SIZE];
        long long      tb[BUFFER_SIZE];
        double         vb[BUFFER_SIZE];

        VectorSP kvec = keysVec;
        VectorSP tsv  = tsVec;
        VectorSP vvec = valVec;

        INDEX rest = N;
        INDEX nread = 0;
        int   read  = 0;

        for (; rest > 0; nread += read, rest = N - nread) {
            // 3) 从 DolphinDB 一次拉一段到本地缓冲（同 batchSetPipe）
            if (rest >= BUFFER_SIZE) {
                kvec->getStringConst(nread, BUFFER_SIZE, kb);
                tsv ->getLong      (nread, BUFFER_SIZE, tb);
                vvec->getDouble    (nread, BUFFER_SIZE, vb);
                read = BUFFER_SIZE;
            } else {
                kvec->getStringConst(nread, rest, kb);
                tsv ->getLong      (nread, rest, tb);
                vvec->getDouble    (nread, rest, vb);
                read = rest;
            }

            // 4) 对这段数据打成多条 TS.MADD，并 pipeline 追加；到达阈值后统一 drain
            int sentInBatch = 0;

            for (int off = 0; off < read; /*递增见下*/) {
                const int take = std::min(TUPLES_PER_CMD, read - off);

                // 构建一条 TS.MADD: ["TS.MADD", k1, ts1, v1, ..., kt, tst, vt]
                std::vector<std::string> holder;
                holder.reserve(1 + take * 3);
                holder.emplace_back("TS.MADD");
                for (int i = 0; i < take; ++i) {
                    holder.emplace_back(kb[off + i]->getString());
                    holder.emplace_back(std::to_string(toMillis(isNanoTs, tb[off + i])));
                    holder.emplace_back(d2str(vb[off + i]));
                }

                std::vector<const char*> argv(holder.size());
                std::vector<size_t>      argvlen(holder.size());
                for (size_t i = 0; i < holder.size(); ++i) {
                    argv[i]    = holder[i].c_str();
                    argvlen[i] = holder[i].size();
                }

                if (redisAppendCommandArgv(redisConnect_, (int)argv.size(), argv.data(), argvlen.data()) == REDIS_ERR) {
                    throw RuntimeException("[Plugin::Redis] append TS.MADD failed in pipeline");
                }
                ++sentInBatch;
                off += take;

                // 4.1 到达“命令条数”阈值：把这些回复都 drain 掉（完全照搬 batchSetPipe 的 4.2）
                if (sentInBatch == PIPE_BATCH_CMDS) {
                    for (int k = 0; k < sentInBatch; ++k) {
                        redisReply* reply = nullptr;
                        if (redisGetReply(redisConnect_, reinterpret_cast<void**>(&reply)) == REDIS_ERR) {
                            throw RuntimeException("[Plugin::Redis] getReply failed in pipeline (TS.MADD)");
                        }
                        RedisReplyGuard replyGuard(reply);
                        // 这里只需要检查错误；返回类型通常是 ARRAY（每三元组一个时间戳）
                        checkReply(reply, "TS.MADD");
                    }
                    sentInBatch = 0;
                }
            }

            // 4.2 把本段最后不足一批的也 drain 掉（照搬 batchSetPipe 的 4.3）
            if (sentInBatch > 0) {
                for (int k = 0; k < sentInBatch; ++k) {
                    redisReply* reply = nullptr;
                    if (redisGetReply(redisConnect_, reinterpret_cast<void**>(&reply)) == REDIS_ERR) {
                        throw RuntimeException("[Plugin::Redis] getReply failed in pipeline (TS.MADD)");
                    }
                    RedisReplyGuard replyGuard(reply);
                    checkReply(reply, "TS.MADD");
                }
            }
        }

        // 5) 完成（与 batchSetPipe 一致返回风格）
        return new String("tsMAdd finish (pipelined).");
    }

}
