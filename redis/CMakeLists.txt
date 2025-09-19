cmake_minimum_required(VERSION 3.22)

set(PluginName PluginRedisPro)
project(${PluginName})

include(../template.cmake)
CreatePlugin(${PluginName})

target_compile_definitions(${PluginName} PRIVATE _GLIBCXX_USE_CXX11_ABI=0) # Add ABI support

target_compile_options(${PluginName} PRIVATE -Wno-pedantic) # for hiredis third party library

## build hiredis
SET(DISABLE_TESTS ON CACHE BOOL "stop build redis tests")
add_subdirectory(contrib/hiredis)

target_link_libraries(${PluginName} PRIVATE hiredis_static)
