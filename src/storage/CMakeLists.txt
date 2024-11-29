cmake_minimum_required(VERSION 3.22)
project(Database)

set(CMAKE_CXX_STANDARD 17)

include_directories(index)
include_directories(table)

add_executable(
        Database
        index/bplus_tree.cpp
        index/bplus_index.cpp
        table/table.cpp
)