cmake_minimum_required(VERSION 4.2)
project(Coroutine)

set(CMAKE_CXX_STANDARD 23)

add_library(asan_profile INTERFACE)

option(ENABLE_ASAN "Enable AddressSanitizer" ON)

if(ENABLE_ASAN)
    if(MSVC)
#        target_compile_options(asan_profile INTERFACE /fsanitize=address)
#        target_link_options(asan_profile INTERFACE /fsanitize=address)

    else()
        
        target_compile_options(asan_profile INTERFACE -fsanitize=address -fno-omit-frame-pointer)
        target_link_options(asan_profile INTERFACE -fsanitize=address)
    endif()
endif()

add_executable(Coroutine main.cpp
        coroutine.hpp)

target_link_libraries(Coroutine PRIVATE asan_profile)

# target is 64 bits
#target_compile_options(Coroutine PRIVATE -m64)

# target must be statically linked (gcc need to static link libstdc++)
#target_link_options(Coroutine PUBLIC -static -static-libgcc -static-libstdc++)

#if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
#    add_library(coro_ir_dummy OBJECT EXCLUDE_FROM_ALL ${CMAKE_SOURCE_DIR}/main.cpp)
#    target_link_libraries(coro_ir_dummy PRIVATE YourTarget)
#    target_compile_options(coro_ir_dummy PRIVATE -S -emit-llvm)
#    set_target_properties(coro_ir_dummy PROPERTIES
#            OBJECT_OUTPUTS ${CMAKE_BINARY_DIR}/coro_debug/main.ll
#    )
#
#    add_library(coro_ir_dummy_raw OBJECT EXCLUDE_FROM_ALL ${CMAKE_SOURCE_DIR}/main.cpp)
#    target_link_libraries(coro_ir_dummy_raw PRIVATE YourTarget)
#    target_compile_options(coro_ir_dummy_raw PRIVATE -S -emit-llvm -Xclang -disable-llvm-passes)
#endif()