#CMakeのバージョンを設定
cmake_minimum_required(VERSION 3.10)

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -O0")

# プロジェクト名と使用する言語を設定
project(ccbench_ss2pl
        VERSION 0.0.1
        DESCRIPTION "ss2pl of ccbench"
        LANGUAGES CXX)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/../cmake")

find_package(gflags REQUIRED)
find_package(Threads REQUIRED)

# a.outという実行ファイルをmain.cppとhello.cppから作成
file(GLOB SS2PL_SOURCES
        "ss2pl.cc"
        )
add_executable(ss2pl.exe ${SS2PL_SOURCES})

target_link_libraries(ss2pl.exe
        gflags::gflags
        Threads::Threads
        )
