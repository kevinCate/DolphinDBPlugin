find_package(hiredis QUIET)
if(hiredis_FOUND)
    list(APPEND REDIS_PLUS_PLUS_HIREDIS_LIBS hiredis::hiredis)

    if(NOT hiredis_INCLUDE_DIRS)
        # This can happen when hiredis is included with FetchContent with OVERRIDE_FIND_PACKAGE
        find_path(
          hiredis_INCLUDE_DIRS
          hiredis.h
          PATHS
            ${CMAKE_BINARY_DIR}/_deps/hiredis
            ${CMAKE_CURRENT_BINARY_DIR}/_deps/hiredis
          NO_CACHE
          REQUIRED
        )

        # Remove the trailing /hiredis from the include path so that we can include hiredis with hiredis/hiredis.h
        get_filename_component(hiredis_INCLUDE_DIRS "${hiredis_INCLUDE_DIRS}" DIRECTORY)
    endif()

    if(REDIS_PLUS_PLUS_USE_TLS)
        find_package(hiredis_ssl REQUIRED)
        list(APPEND REDIS_PLUS_PLUS_HIREDIS_LIBS hiredis::hiredis_ssl)
        find_package(OpenSSL REQUIRED)
        list(APPEND REDIS_PLUS_PLUS_HIREDIS_LIBS ${OPENSSL_LIBRARIES})
    endif()
else()
    # 如果上一层 add_subdirectory() 了 hiredis，就已经有目标可用
    if(TARGET hiredis)
        # 取出接口包含目录（里头可能有 $<BUILD_INTERFACE:...>; $<INSTALL_INTERFACE:...>）
        get_target_property(_incs hiredis INTERFACE_INCLUDE_DIRECTORIES)

        # 从列表中解析出一个真实可用的 include 前缀
        unset(hiredis_INCLUDE_DIRS)
        foreach(_inc IN LISTS _incs)
            set(_cand "${_inc}")
            if(_cand MATCHES "^\\$<BUILD_INTERFACE:(.+)>$")
                set(_cand "${CMAKE_MATCH_1}")
            elseif(_cand MATCHES "^\\$<INSTALL_INTERFACE:(.+)>$")
                set(_cand "${CMAKE_MATCH_1}")
            endif()
            get_filename_component(_cand "${_cand}" ABSOLUTE)

            if(EXISTS "${_cand}/hiredis/hiredis.h")
                set(hiredis_INCLUDE_DIRS "${_cand}")
                break()
            elseif(EXISTS "${_cand}/hiredis.h")
                # 支持某些工程把 hiredis.h 直接放在该目录
                get_filename_component(hiredis_INCLUDE_DIRS "${_cand}" DIRECTORY)
                break()
            endif()
        endforeach()

        # 兜底：按你的树形结构再给一个保守值（可按需调整）
        if(NOT hiredis_INCLUDE_DIRS)
            set(hiredis_INCLUDE_DIRS "${CMAKE_CURRENT_LIST_DIR}/../../hiredis")
        endif()

        list(APPEND REDIS_PLUS_PLUS_HIREDIS_LIBS hiredis)
        set(hiredis_FOUND ON)

    else()
        # 没有目标时，才回退到查头+库
        find_path(HIREDIS_HEADER
                NAMES hiredis/hiredis.h
                HINTS
                ${CMAKE_CURRENT_LIST_DIR}/../../contrib
                ${CMAKE_CURRENT_BINARY_DIR}/_deps/hiredis
                ${CMAKE_BINARY_DIR}/_deps/hiredis
                PATH_SUFFIXES include
                NO_CACHE
                REQUIRED)

        find_library(HIREDIS_LIB
                NAMES hiredis hiredis_static            # 兼容 libhiredis_static.a
                HINTS
                ${CMAKE_BINARY_DIR}/contrib/hiredis
                ${CMAKE_CURRENT_LIST_DIR}/../../contrib/hiredis
                NO_CACHE
                REQUIRED)

        list(APPEND REDIS_PLUS_PLUS_HIREDIS_LIBS ${HIREDIS_LIB})
    endif()

    if(REDIS_PLUS_PLUS_USE_TLS)
        find_library(HIREDIS_TLS_LIB hiredis_ssl
                HINTS ${CMAKE_BINARY_DIR}/contrib/hiredis ${CMAKE_CURRENT_LIST_DIR}/../../contrib/hiredis
                NO_CACHE)
        if(HIREDIS_TLS_LIB)
            list(APPEND REDIS_PLUS_PLUS_HIREDIS_LIBS ${HIREDIS_TLS_LIB})
        else()
            find_package(hiredis_ssl REQUIRED)
            list(APPEND REDIS_PLUS_PLUS_HIREDIS_LIBS hiredis::hiredis_ssl)
        endif()
        find_package(OpenSSL REQUIRED)
        list(APPEND REDIS_PLUS_PLUS_HIREDIS_LIBS ${OPENSSL_LIBRARIES})
    endif()
endif()

message(STATUS "[FindHiredis] hiredis_FOUND=${hiredis_FOUND}")
message(STATUS "[FindHiredis] hiredis_INCLUDE_DIRS=${hiredis_INCLUDE_DIRS}")
message(STATUS "[FindHiredis] REDIS_PLUS_PLUS_HIREDIS_LIBS=${REDIS_PLUS_PLUS_HIREDIS_LIBS}")