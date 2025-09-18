cmake_minimum_required(VERSION 3.22)
project(DolphinDBPluginsSuperbuild LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)

# （可选）如果顶层也需要用到 template.cmake：
# include("${CMAKE_CURRENT_LIST_DIR}/template.cmake" OPTIONAL)

# 只把 redis 子目录加入构建（以后还可以 add_subdirectory(kafka) 等）
add_subdirectory(redis)