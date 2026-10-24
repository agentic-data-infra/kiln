# Not built. scripts/gen_third_party.py configures this project only to read
# the compile flags and link lines CMake computes for the installed fbthrift
# (and with it wangle, fizz, mvfst, folly, glog and gflags) and for gtest.
cmake_minimum_required(VERSION 3.20)
project(deps_probe CXX)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

find_package(wangle CONFIG REQUIRED)
find_package(FBThrift CONFIG REQUIRED)
find_package(GTest REQUIRED)

add_executable(fbthrift_probe main.cpp)
target_link_libraries(fbthrift_probe FBThrift::thriftcpp2)

add_executable(gtest_probe main.cpp)
target_link_libraries(gtest_probe GTest::gtest)
