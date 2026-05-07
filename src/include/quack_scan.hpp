#pragma once

#include "quack_uri.hpp"
#include "quack_client.hpp"

namespace duckdb {

struct QuackScanBindData : FunctionData {
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<QuackScanBindData>();
		return other.connection_id == connection_id && other.server_uri == server_uri &&
		       other.table_name == table_name && other.column_names == column_names &&
		       other.column_types == column_types && other.needs_more_fetch == needs_more_fetch;
	}
	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<QuackScanBindData>();
		result->connection_id = connection_id;
		result->server_uri = server_uri;
		result->table_name = table_name;
		result->column_names = column_names;
		result->column_types = column_types;
		result->needs_more_fetch = needs_more_fetch;
		return std::move(result);
	}

	string connection_id;
	QuackUri server_uri;
	string table_name;
	unique_ptr<QuackClient> initial_client;
	vector<string> column_names;
	vector<LogicalType> column_types;
	vector<unique_ptr<DataChunkWrapper>> results;
	bool needs_more_fetch;
	mutex lock;
};

class TableFunction;

class QuackScanFunction {
public:
	static TableFunction GetFunction();
};

class QuackScanByNameFunction {
public:
	static TableFunction GetFunction();
};

} // namespace duckdb
