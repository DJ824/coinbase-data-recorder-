cmake_minimum_required(VERSION 3.16)
project(data_writer)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find required packages
find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBWEBSOCKETS REQUIRED libwebsockets)
find_package(CURL REQUIRED)

add_executable(data_writer
        main.cpp
        l2_writer.cpp
        l2_writer.h
        spsc.h
        coinbase_feed.cpp
        coinbase_feed.h
)

# Link libraries
target_link_libraries(data_writer
        PRIVATE
        ${LIBWEBSOCKETS_LIBRARIES}
        ${CURL_LIBRARIES}
        pthread
)

# Include directories
target_include_directories(data_writer
        PRIVATE
        ${LIBWEBSOCKETS_INCLUDE_DIRS}
        ${CURL_INCLUDE_DIRS}
)

# Compiler flags
target_compile_options(data_writer
        PRIVATE
        ${LIBWEBSOCKETS_CFLAGS_OTHER}
        ${CURL_CFLAGS_OTHER}
)