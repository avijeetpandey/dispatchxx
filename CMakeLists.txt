cmake_minimum_required(VERSION 3.20)
project(dispatchxx LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
endif()

option(DISPATCHXX_BUILD_TESTS "Build the dispatchxx test suite" ON)
option(DISPATCHXX_SANITIZE_THREAD "Build with ThreadSanitizer" OFF)

if(DISPATCHXX_SANITIZE_THREAD)
    add_compile_options(-fsanitize=thread -g -O1)
    add_link_options(-fsanitize=thread)
endif()

find_package(Threads REQUIRED)

add_library(dispatchxx
    src/InMemoryBroker.cpp
    src/TaskRegistry.cpp
)
target_include_directories(dispatchxx PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(dispatchxx PUBLIC Threads::Threads)
target_compile_options(dispatchxx PRIVATE -Wall -Wextra -Wpedantic)

if(DISPATCHXX_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
