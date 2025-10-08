//
// Created by uplee on 9/25/25.
//

#pragma once
#include "rc_connection.h"
#include "ddbplugin/Plugin.h"
#include <unordered_map>
#include <mutex>

namespace rc {

// 资源名
extern const std::string RC_HANDLE_NAME;

// 插件的全局句柄表（由 plugin.cpp 定义、此处仅声明）
extern ddb::BackgroundResourceMap<RedisClusterConn> g_rc_map;

ddb::SmartPointer<RedisClusterConn> getConn(const ddb::ConstantSP& h);

// token -> handle registry for getHandle / getHandleStatus
extern std::unordered_map<std::string, ddb::ConstantSP> g_token2handle;

extern std::mutex g_token_mu;

}