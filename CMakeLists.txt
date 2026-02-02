cmake_minimum_required(VERSION 3.8)
project(raft)

set(CMAKE_CXX_STANDARD 17)

message(STATUS "AddressSanitizer ENABLED")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer -g")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address -fno-omit-frame-pointer -g")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fsanitize=address")

# Disable RocksDB tracing features to avoid null handler issues
add_compile_definitions(ROCKSDB_LITE)

if(${LOG} STREQUAL on)
  add_compile_definitions(ENABLE_LOG)
endif()

# enable recording more fine granularity performance records
if(${PERF} STREQUAL on)
  add_compile_definitions(ENABLE_PERF_RECORDING)
endif()

include_directories(${CMAKE_SOURCE_DIR}/RCF/include)
add_subdirectory(${CMAKE_SOURCE_DIR}/RCF)

include_directories(${CMAKE_SOURCE_DIR}/third-party/leveldb/include)
include_directories(/opt/homebrew/include)
link_directories(/opt/homebrew/lib)

add_subdirectory(${CMAKE_SOURCE_DIR}/third-party)
add_subdirectory(${CMAKE_SOURCE_DIR}/raft)
add_subdirectory(${CMAKE_SOURCE_DIR}/kv)
add_subdirectory(${CMAKE_SOURCE_DIR}/bench)
