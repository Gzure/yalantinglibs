set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/output/examples/coro_rpc)
if (YLT_ENABLE_URMA)
  add_executable(coro_rpc_urma_benchmark
      urma_benchmark.cpp
    )
  add_executable(coro_rpc_urma_minimal_test
      urma_minimal_test.cpp
    )
  target_link_libraries(coro_rpc_urma_minimal_test PRIVATE yalantinglibs::yalantinglibs)
else()
  add_custom_target(coro_rpc_urma_benchmark
    COMMAND ${CMAKE_COMMAND} -E echo
            "coro_rpc_urma_benchmark requires -DYLT_ENABLE_URMA=ON. Re-run CMake configure with URMA enabled."
    COMMAND ${CMAKE_COMMAND} -E false
  )
endif()
