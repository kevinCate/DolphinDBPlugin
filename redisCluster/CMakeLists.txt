cmake_minimum_required(VERSION 3.22)
project(PluginRedisCluster LANGUAGES CXX)

# 统一标准与 PIC（静态库要能进 .so）
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# ====== 先把第三方库加进来（本地子树）======
# 1) hiredis
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)    # 让 hiredis 生成静态库 libhiredis.a
set(CMAKE_POSITION_INDEPENDENT_CODE ON)           # 静态库也用 -fPIC，才能被插件(.so)链接

add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/contrib/hiredis
        ${CMAKE_BINARY_DIR}/contrib/hiredis)

# 2) redis-plus-plus：只编静态、C++17、关测试、静态带 PIC
set(REDIS_PLUS_PLUS_CXX_STANDARD 17 CACHE STRING "" FORCE)
set(REDIS_PLUS_PLUS_BUILD_TEST OFF CACHE BOOL "" FORCE)
set(REDIS_PLUS_PLUS_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(REDIS_PLUS_PLUS_BUILD_STATIC ON  CACHE BOOL "" FORCE)
set(REDIS_PLUS_PLUS_BUILD_STATIC_WITH_PIC ON CACHE BOOL "" FORCE)
# tell FindHiredis：已找到，并给出 include 路径与库
set(hiredis_FOUND ON CACHE BOOL "" FORCE)
set(hiredis_INCLUDE_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/contrib CACHE PATH "" FORCE)
set(HIREDIS_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/contrib CACHE PATH "" FORCE)
set(REDIS_PLUS_PLUS_HIREDIS_LIBS hiredis CACHE STRING "" FORCE)

# 如需 TLS/异步后续可打开：
# set(REDIS_PLUS_PLUS_USE_TLS ON CACHE BOOL "" FORCE)
# set(REDIS_PLUS_PLUS_BUILD_ASYNC libuv CACHE STRING "" FORCE)

SET(DISABLE_TESTS ON CACHE BOOL "stop build redis tests")

add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/contrib/redis-plus-plus
        ${CMAKE_BINARY_DIR}/contrib/redis-plus-plus)

# ====== 你的模板（保留）======
include(../template.cmake)

# 创建插件目标并下钻到 src/
# 可选第二个参数为 LOG_NAME（你原宏支持），不需要可不传
CreatePlugin(PluginRedisCluster)

set_property(TARGET PluginRedisCluster PROPERTY CXX_STANDARD 17)
set_property(TARGET PluginRedisCluster PROPERTY CXX_STANDARD_REQUIRED ON)

target_compile_options(PluginRedisCluster PRIVATE -Wno-pedantic) # for hiredis third party library
