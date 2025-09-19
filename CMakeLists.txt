cmake_minimum_required(VERSION 3.22)
project(DolphinDBPlugin LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)

add_subdirectory(redis) # Only add redis folder for compilation