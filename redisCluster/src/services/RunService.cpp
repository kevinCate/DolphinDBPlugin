#include "services/RunService.h"
#include "core/ConnFacade.h"
#include <sw/redis++/redis++.h>
#include "hiredis.h"
#include <cstring>

using ddb::ConstantSP;
using ddb::IllegalArgumentException;
using ddb::RuntimeException;
using ddb::String;
using ddb::Util;
using ddb::VectorSP;
using ddb::Void;

namespace rc {

// NOLINTNEXTLINE(misc-no-recursion)
ConstantSP RunService::convertReply(const redisReply* reply) { //
    if (reply == nullptr) return new Void();

    switch (reply->type) {
        case REDIS_REPLY_ERROR:
            throw RuntimeException(
                std::string("[Plugin::RedisCluster] Redis reply error: ") + reply->str);

        case REDIS_REPLY_STRING:
        case REDIS_REPLY_STATUS:
        case REDIS_REPLY_BIGNUM:
        case REDIS_REPLY_VERB:
            return new String(ddb::DolphinString(reply->str, reply->len));

        case REDIS_REPLY_NIL:
            return new Void();

        case REDIS_REPLY_INTEGER:
            return new ddb::Long(reply->integer);

        case REDIS_REPLY_DOUBLE:
            return new ddb::Double(reply->dval);

        case REDIS_REPLY_BOOL:
            return new ddb::Bool(static_cast<char>(reply->integer != 0));

        case REDIS_REPLY_ARRAY: {
            const size_t n = reply->elements;
            VectorSP vec  = Util::createVector(ddb::DT_ANY, static_cast<ddb::INDEX>(n));
            for (size_t i = 0; i < n; ++i) {
                vec->set(static_cast<ddb::INDEX>(i), convertReply(reply->element[i]));
            }
            return vec;
        }

        default:
            throw RuntimeException(
                "[Plugin::RedisCluster] Unsupported redis reply type: " +
                std::to_string(reply->type) + ".");
    }
}

void RunService::checkReply(const redisReply* reply, const std::string& cmdName) {
    if (reply == nullptr) {
        throw RuntimeException("[Plugin::RedisCluster] Invalid redis reply.");
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        throw RuntimeException(
            "[Plugin::RedisCluster] " + cmdName + " failed: " +
            std::string(reply->str));
    }
}

ConstantSP RunService::run(const std::string& routeKey,
                                const std::vector<std::string>& cmd) const {
    if (routeKey.empty())
        throw IllegalArgumentException(
            __FUNCTION__, "[Plugin::RedisCluster] routeKey (hash-tag/key) must be non-empty.");
    if (cmd.empty())
        throw IllegalArgumentException(
            __FUNCTION__, "[Plugin::RedisCluster] argv must contain at least a command name.");

    std::vector<const char*> argv;
    std::vector<size_t>      argvlen;
    argv.reserve(cmd.size());
    argvlen.reserve(cmd.size());
    for (const auto& s : cmd) {
        argv.push_back(s.c_str());
        argvlen.push_back(s.size());
    }

    const std::string& cmdName = cmd[0];

    auto& cluster = conn_.rc();
    auto   redis  = cluster.redis(sw::redis::StringView(routeKey));

    sw::redis::ReplyUPtr rp;
    try {
        rp = redis.command(argv.begin(), argv.end());
    } catch (const std::exception& e) {
        throw RuntimeException(
            std::string("[Plugin::RedisCluster] run command failed: ") + e.what());
    }

    if (!rp) return new Void();

    redisReply* raw = rp.get();
    checkReply(raw, cmdName);
    return convertReply(raw);
}

}  // namespace rc