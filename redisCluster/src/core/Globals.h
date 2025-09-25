//
// Created by uplee on 9/25/25.
//

#ifndef GLOBALS_H
#define GLOBALS_H

#pragma once
#include "DolphinDBEverything.h"
#include "rc_connection.h"
#include "ddbplugin/Plugin.h"

// 资源名
extern const std::string RC_HANDLE_NAME;

// 插件的全局句柄表（由 plugin.cpp 定义、此处仅声明）
extern ddb::BackgroundResourceMap<RedisClusterConn> g_rc_map;

#endif //GLOBALS_H
