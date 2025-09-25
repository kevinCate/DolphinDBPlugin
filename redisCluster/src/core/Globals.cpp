//
// Created by uplee on 9/25/25.
//

#include "core/Globals.h"
#include "rc_connection.h"

const std::string RC_HANDLE_NAME = "redis cluster";
ddb::BackgroundResourceMap<RedisClusterConn> g_rc_map("[Plugin::RedisCluster] ", RC_HANDLE_NAME);