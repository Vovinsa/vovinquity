cmake_minimum_required(VERSION 3.22)
project(Database)

set(CMAKE_CXX_STANDARD 17)

include_directories(src/storage/index)
include_directories(src/storage/table)
include_directories(src/storage/table/system)
include_directories(src/catalog)
include_directories(src/planner)
include_directories(src/executor)
include_directories(src/parser)

add_executable(
        Database
        src/storage/index/bplus_tree.cpp
        src/storage/index/bplus_index.cpp
        src/storage/table/table.cpp
        src/storage/table/system/system_table.cpp
        src/catalog/catalog.cpp
        src/planner/planner.cpp
        src/executor/executor.cpp
        src/parser/parser.cpp
        main.cpp
)

target_link_libraries(Database PRIVATE readline ncurses)