target_include_directories(${CMAKE_PROJECT_NAME}
    PUBLIC ${PROJECT_SOURCE_DIR}/include/lz4
)

target_sources(${CMAKE_PROJECT_NAME} PRIVATE
    lz4.c
)
