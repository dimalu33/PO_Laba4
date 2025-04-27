cmake_minimum_required(VERSION 3.28)
project(lab4)

set(CMAKE_CXX_STANDARD 17)

add_executable(lab4 server.cpp)

target_link_libraries(lab4 ws2_32)
