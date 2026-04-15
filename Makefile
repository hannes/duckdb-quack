PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=rpc
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

DEFAULT_TEST_EXTENSION_DEPS=httpfs;

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile
