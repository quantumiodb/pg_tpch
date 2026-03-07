
cmake_minimum_required(VERSION 3.20.0)
project(pg_tpch VERSION
  1.0.0
  LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_EXPORT_COMPILE_COMMANDS on)
set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)

find_package(PostgreSQL REQUIRED)

# runtime lib dir for libpq
set(CMAKE_INSTALL_RPATH ${PostgreSQL_LIBRARY_DIRS})

enable_testing()

# clang-format
add_custom_target(format
  COMMAND git diff --name-only --diff-filter=d --cached '*.cpp' '*.h' | xargs clang-format -i
  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
)

file(
  GLOB_RECURSE
  TPCH_SRCS
  CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/src/tpch/*.cpp 
)

add_library(tpch
  OBJECT
    ${TPCH_SRCS}
)

target_include_directories(tpch
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/src/tpch/include
    ${CMAKE_CURRENT_SOURCE_DIR}/src/tpch/include/dbgen
    ${PostgreSQL_SERVER_INCLUDE_DIRS}
)

# disable some compile error from tpch
target_compile_options(tpch
  PUBLIC
    "-fPIC"
    "-Wno-write-strings"
    "-Wno-format-truncation"
)

add_postgresql_extension(
  pg_tpch
  VERSION 1.0
  SOURCES src/pg_tpch.cpp
  SCRIPTS src/pg_tpch--1.0.sql
)

if(NOT DEFINED TPCH_ANSWERS_DIR)
  set(TPCH_ANSWERS_DIR ${CMAKE_CURRENT_SOURCE_DIR}/answers)
endif()
target_compile_definitions(tpch PRIVATE TPCH_ANSWERS_DIR="${TPCH_ANSWERS_DIR}")

target_link_libraries(pg_tpch tpch pq)

cmake_host_system_information(RESULT core_num QUERY NUMBER_OF_LOGICAL_CORES)

file(APPEND
  ${CMAKE_CURRENT_SOURCE_DIR}/src/pre_prepare.sql
  "update tpch.tpch_host_info set host_core = ${core_num};\n")

set(TPCH_CURR_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src/tpch)
set(TPCH_INSTALL_DIR ${PostgreSQL_EXTENSION_DIR}/tpch)

install(
  DIRECTORY
    ${TPCH_CURR_DIR}/schema
  DESTINATION
    ${TPCH_INSTALL_DIR}
)

install(
  DIRECTORY
    ${TPCH_CURR_DIR}/queries
  DESTINATION
    ${TPCH_INSTALL_DIR}
)

install(
  FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/src/pre_prepare.sql
    ${CMAKE_CURRENT_SOURCE_DIR}/src/post_prepare.sql
  DESTINATION
    ${TPCH_INSTALL_DIR}
)
