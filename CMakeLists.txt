# Copyright 2023 Chengye YU <yuchengye2013 AT outlook.com>.
# SPDX-License-Identifier: Apache-2.0

cmake_minimum_required(VERSION 3.12)
project(ycsb-cpp LANGUAGES C CXX)
set(CMAKE_CXX_STANDARD 17)

file(GLOB_RECURSE YCSB_CORE_SRC "core/*.cc")

add_executable(ycsb ${YCSB_CORE_SRC})
target_include_directories(ycsb PRIVATE ${PROJECT_SOURCE_DIR})

include(FindThreads)
find_package(gflags CONFIG REQUIRED)

message(STATUS "BIND_ROCKSDB - ON")
message(STATUS "BIND_CACHELIB - ON")
find_package(cachelib CONFIG REQUIRED)
find_package(RocksDB CONFIG REQUIRED)
file(GLOB_RECURSE YCSB_CACHELIB_SRC "cachelib/*.cc")
target_sources(ycsb PRIVATE ${YCSB_CACHELIB_SRC})
target_link_libraries(ycsb PRIVATE cachelib rocksdb)

add_subdirectory(HdrHistogram_c)
include_directories(HdrHistogram_c/include)
#add_compile_definitions(HDRMEASUREMENT)
#add_dependencies(ycsb hdr_histogram)
#target_link_libraries(ycsb PRIVATE hdr_histogram)
