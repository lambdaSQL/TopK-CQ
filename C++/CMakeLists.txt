cmake_minimum_required(VERSION 3.0.0)
project(Relational LANGUAGES CXX)

if (CMAKE_BUILD_TYPE STREQUAL "")
    message(STATUS "Build type was unspecified, set to Release")
    set(CMAKE_BUILD_TYPE Release)
else ()
    message(STATUS "Build type specified as ${CMAKE_BUILD_TYPE}")
endif ()

set(CMAKE_CXX_FLAGS "-Wall -Wextra")
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

set(INCLUDE_DIR "${PROJECT_SOURCE_DIR}/src")
set(DATA_DIR "${PROJECT_SOURCE_DIR}/data")

add_subdirectory(src)

include(CTest)
enable_testing()
add_subdirectory(test)