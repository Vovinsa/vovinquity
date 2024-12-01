cmake_minimum_required(VERSION 3.22)
project(Database)

set(CMAKE_CXX_STANDARD 17)

include_directories(src/storage/index)
include_directories(src/storage/table)

add_executable(
        Database
        src/storage/index/bplus_tree.cpp
        src/storage/index/bplus_index.cpp
        src/storage/table/table.cpp
        main.cpp
)