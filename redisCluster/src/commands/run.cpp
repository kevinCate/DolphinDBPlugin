#include "commands/run.h"
#include "core/ConnFacade.h"
#include "core/Globals.h"
#include "core/Router.h"
#include "services/RunService.h"
#include <string>
#include <vector>

using ddb::ConstantSP;
using ddb::IllegalArgumentException;
using ddb::RuntimeException;
using ddb::VectorSP;

namespace {

constexpr int kMaxCmdTokens = 4096;

bool starts_with(const std::string& s, const char* pfx) {
    return s.rfind(pfx, 0) == 0;
}

// Parse new signature: routeKey is a STRING scalar, cmd is STRING VECTOR.
void parse_route_and_command(const ConstantSP& routeArg,
                                const VectorSP& cmdVec,
                                std::string& routeKey,
                                std::vector<std::string>& cmd) {
    // --- routeKey checks ---
    if (routeArg->getType() != ddb::DT_STRING || routeArg->isVector())
        throw IllegalArgumentException(
            __FUNCTION__, "[Plugin::RedisCluster] routeKey must be a STRING scalar.");
    // --- command vector checks ---
    if (cmdVec->getType() != ddb::DT_STRING)
        throw IllegalArgumentException(
            __FUNCTION__, "[Plugin::RedisCluster] command must be a STRING vector.");
    const int n = cmdVec->size();
    if (n <= 0)
        throw IllegalArgumentException(
            __FUNCTION__, "[Plugin::RedisCluster] command vector must not be empty.");
    if (n > kMaxCmdTokens) {
        throw IllegalArgumentException(
            __FUNCTION__, "[Plugin::RedisCluster] command vector length must not exceed 4096.");
    }

    // pull routeKey text
    std::string head = routeArg->getString();

    // accept "tag=...", "tag:...", "key=...", "key:..."
    if (starts_with(head, "tag=") || starts_with(head, "tag:") ||
        starts_with(head, "key=") || starts_with(head, "key:")) {
        routeKey = head.substr(4);
    } else {
        routeKey = head;
    }

    // if routeKey is like "...{mytag}...", use the hash-tag content as routeKey
    if (auto tag_view = rc::extract_tag(routeKey); !tag_view.empty()) {
        routeKey.assign(tag_view.begin(), tag_view.end());
    }

    // copy command tokens
    cmd.clear();
    cmd.reserve(static_cast<std::size_t>(n));
    for (int i = 0; i < n; ++i) {
        cmd.emplace_back(cmdVec->getString(i));
    }

    // final sanity
    if (cmd.empty())
        throw IllegalArgumentException(
            __FUNCTION__, "[Plugin::RedisCluster] missing redis command tokens.");
}

}  // anonymous namespace

// New signature: run(conn, routeKey: STRING, cmd: STRING VECTOR)
ConstantSP ddb_rc_run(ddb::Heap*, const std::vector<ddb::ConstantSP>& args) {
    if (args.size() < 3 || !args[2]->isVector())
        throw IllegalArgumentException(
            __FUNCTION__, "Usage: run(conn, routeKey: STRING, cmd: STRING VECTOR)");

    auto conn = rc::getConn(args[0]);
    rc::ConnFacade cf(*conn);

    std::string              routeKey;
    std::vector<std::string> cmd;
    parse_route_and_command(/*routeArg*/ args[1], /*cmdVec*/ args[2], routeKey, cmd);

    const rc::RunService svc(cf);
    try {
        return svc.run(routeKey, cmd);
    } catch (const std::exception& e) {
        throw RuntimeException(std::string("[Plugin::RedisCluster] run failed: ") + e.what());
    }
}