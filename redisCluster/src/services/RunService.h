#ifndef RUNSERVICE_H
#define RUNSERVICE_H
#pragma once
#include "ddbplugin/Plugin.h"
#include "hiredis.h"
#include <string>
#include <vector>

namespace rc {

class ConnFacade;  // forward declaration

class RunService {
public:
    explicit RunService(ConnFacade& cf) : conn_(cf) {}

    // Execute a single command on the node indicated by routeKey (hash-tag or key).
    // cmd = ["COMMAND", "arg1", ...]
    [[nodiscard]] ddb::ConstantSP run(const std::string& routeKey, const std::vector<std::string>& cmd) const;

private:
    static ddb::ConstantSP convertReply(const redisReply* reply);
    static void            checkReply(const redisReply* reply,
                                      const std::string& cmdName);

private:
    ConnFacade& conn_;
};

}  // namespace rc

#endif  // RUNSERVICE_H