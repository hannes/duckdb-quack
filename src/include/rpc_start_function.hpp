#pragma once

namespace duckdb {

class TableFunction;

class RpcStartFunction {
public:
	static TableFunction GetFunction();
};

class RpcStopFunction {
public:
	static TableFunction GetFunction();
};

class RpcGenerateKeysFunction {
public:
	static TableFunction GetFunction();
};
} // namespace duckdb
