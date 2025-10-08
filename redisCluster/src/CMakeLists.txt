# 把你的源文件加到目标里（可继续追加更多 .cpp）
target_sources(${PluginName}
        PRIVATE
        ${CMAKE_CURRENT_LIST_DIR}/plugin.cpp
        ${CMAKE_CURRENT_LIST_DIR}/core/Globals.cpp
        ${CMAKE_CURRENT_LIST_DIR}/core/Router.cpp
        ${CMAKE_CURRENT_LIST_DIR}/core/RedisTaskDispatcher.cpp
        ${CMAKE_CURRENT_LIST_DIR}/commands/kv.cpp
        ${CMAKE_CURRENT_LIST_DIR}/commands/batch.cpp
        ${CMAKE_CURRENT_LIST_DIR}/commands/run.cpp
        ${CMAKE_CURRENT_LIST_DIR}/commands/list.cpp
        ${CMAKE_CURRENT_LIST_DIR}/services/BatchService.cpp
        ${CMAKE_CURRENT_LIST_DIR}/services/KVService.cpp
        ${CMAKE_CURRENT_LIST_DIR}/services/RunService.cpp
        ${CMAKE_CURRENT_LIST_DIR}/services/ListService.cpp
)

target_include_directories(${PluginName}
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}        # src/
        ${CMAKE_CURRENT_SOURCE_DIR}/core   # src/core/
        ${CMAKE_CURRENT_SOURCE_DIR}/commands   # src/commands/
)

# 如果需要额外包含目录，也可以在这里加：
# target_include_directories(${PluginName} PRIVATE ${CMAKE_CURRENT_LIST_DIR})

# 链接到我们刚才 add_subdirectory 进来的静态库
target_link_libraries(${PluginName}
        PRIVATE
        redis++_static
        hiredis
        # 如需 pthread（通常 redis++ 用得到）
        pthread
)

# 若你的 DolphinDB 服务器要求旧 libstdc++ ABI，请取消注释下一行，并保证三方同一 ABI
target_compile_definitions(${PluginName} PRIVATE
        LINUX
        LOCKFREE_SYMBASE
        _GLIBCXX_USE_CXX11_ABI=0
)