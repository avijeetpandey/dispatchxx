include(FetchContent)

FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.15.2
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

add_executable(dispatchxx_tests
    test_in_memory_broker.cpp
)
target_link_libraries(dispatchxx_tests PRIVATE dispatchxx GTest::gtest_main)
target_compile_options(dispatchxx_tests PRIVATE -Wall -Wextra)

include(GoogleTest)
gtest_discover_tests(dispatchxx_tests)
