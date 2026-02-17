#pragma once

namespace duckdb {

class ScalarFunction;

class RpcStartFunction {
public:
	static ScalarFunction GetFunction();
};
} // namespace duckdb
