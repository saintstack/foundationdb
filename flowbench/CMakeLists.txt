project (flowbench)

fdb_find_sources(FLOWBENCH_SRCS)

# There is no good way to incorporate the recommended googlebenchmark download + build
# process with one that checks to see if googlebenchmark has already been downloaded
# and built.
if(EXISTS /opt/googlebenchmark-f91b6b AND CLANG AND USE_LIBCXX)
  add_flow_target(EXECUTABLE NAME flowbench SRCS ${FLOWBENCH_SRCS})
  target_include_directories(flowbench PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/include" "${CMAKE_CURRENT_BINARY_DIR}/include" /opt/googlebenchmark-f91b6b/include)
  target_link_directories(flowbench PRIVATE /opt/googlebenchmark-f91b6b/lib64)
elseif(EXISTS /opt/googlebenchmark-f91b6b-g++ AND NOT USE_LIBCXX)
  add_flow_target(EXECUTABLE NAME flowbench SRCS ${FLOWBENCH_SRCS})
  target_include_directories(flowbench PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/include" "${CMAKE_CURRENT_BINARY_DIR}/include" /opt/googlebenchmark-f91b6b-g++/include)
  target_link_directories(flowbench PRIVATE /opt/googlebenchmark-f91b6b-g++/lib64)
else()
  ## Improved Google Benchmark download and build process with better error handling
  ## and compatibility for local development environments.

  # First, try to find system-installed benchmark
  find_package(benchmark QUIET)
  
  if(benchmark_FOUND)
    message(STATUS "Using system-installed Google Benchmark")
    add_flow_target(EXECUTABLE NAME flowbench SRCS ${FLOWBENCH_SRCS})
    target_include_directories(flowbench PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/include" "${CMAKE_CURRENT_BINARY_DIR}/include")
  else()
    message(STATUS "System Google Benchmark not found, building from source")
    
    # Check if we already have the source downloaded
    if(NOT EXISTS ${CMAKE_CURRENT_BINARY_DIR}/googlebenchmark-src)
      # include the configurations from benchmark.cmake
      configure_file(benchmark.cmake googlebenchmark-download/CMakeLists.txt)
      
      # executing the configuration step with better error handling
      execute_process(
          COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
          RESULT_VARIABLE config_result
          OUTPUT_VARIABLE config_output
          ERROR_VARIABLE config_error
          WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googlebenchmark-download
      )
      
      # checking if the configuration step passed
      if(config_result)
          message(WARNING "Google Benchmark configuration failed: ${config_error}")
          message(STATUS "Skipping flowbench target due to Google Benchmark build failure")
          return()
      endif()
      
      # executing the build step with better error handling
      execute_process(
          COMMAND ${CMAKE_COMMAND} --build . --config Release
          RESULT_VARIABLE build_result
          OUTPUT_VARIABLE build_output
          ERROR_VARIABLE build_error
          WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googlebenchmark-download
      )
      
      # checking if the build step passed
      if(build_result)
        message(WARNING "Google Benchmark build failed: ${build_error}")
        message(STATUS "Skipping flowbench target due to Google Benchmark build failure")
        return()
      endif()
    endif()
    
    # Only proceed if the source directory exists
    if(EXISTS ${CMAKE_CURRENT_BINARY_DIR}/googlebenchmark-src)
      set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "Disable benchmark testing" FORCE)
      set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "Disable benchmark gtest tests" FORCE)
      set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "Disable benchmark install" FORCE)
      
      add_subdirectory(
        ${CMAKE_CURRENT_BINARY_DIR}/googlebenchmark-src
        ${CMAKE_CURRENT_BINARY_DIR}/googlebenchmark-build
        EXCLUDE_FROM_ALL
      )
      
      add_flow_target(EXECUTABLE NAME flowbench SRCS ${FLOWBENCH_SRCS})
      target_include_directories(flowbench PRIVATE 
        "${CMAKE_CURRENT_SOURCE_DIR}/include" 
        "${CMAKE_CURRENT_BINARY_DIR}/include"  
        "${CMAKE_CURRENT_BINARY_DIR}/googlebenchmark-src/include"
      )
    else()
      message(STATUS "Google Benchmark source not available, skipping flowbench target")
      return()
    endif()
  endif()
endif()
if(FLOW_USE_ZSTD)
   target_include_directories(flowbench PRIVATE ${ZSTD_LIB_INCLUDE_DIR})
endif()
target_link_libraries(flowbench benchmark pthread flow fdbclient)
