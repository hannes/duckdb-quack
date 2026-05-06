cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME quack)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

#set(CMAKE_CXX_STANDARD 17)

include_directories(src/include duckdb/third_party/httplib duckdb/extension/autocomplete/include)

set(EXTENSION_SOURCES
        src/quack_start_stop.cpp   src/quack_extension.cpp   src/quack_log.cpp         src/quack_server.cpp
        src/quack_catalog.cpp     src/quack_http_server.cpp src/quack_message.cpp     src/quack_storage.cpp
        src/quack_client.cpp      src/quack_insert.cpp      src/quack_scan.cpp src/quack_uri.cpp src/serialize_quack_message.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
