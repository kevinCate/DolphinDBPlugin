cmake_minimum_required(VERSION 3.22)
project(DolphinDBPlugin LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 14)

add_subdirectory(redisCluster) # Only add redis folder for compilation