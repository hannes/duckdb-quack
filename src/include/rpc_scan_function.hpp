#pragma once

namespace duckdb {

class TableFunction;

class RpcScanFunction {
public:
	static TableFunction GetFunction();
};
} // namespace duckdb
