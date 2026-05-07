#pragma once

#include "quack_uri.hpp"
#include "quack_client.hpp"

namespace duckdb {

struct QuackScanBindData : FunctionData {
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<QuackScanBindData>();
		return other.connection_id == connection_id && other.server_uri == server_uri &&
		       other.table_name == table_name && other.column_names == column_names &&
		       other.column_types == column_types;
	}
	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<QuackScanBindData>();
		result->connection_id = connection_id;
		result->server_uri = server_uri;
		result->table_name = table_name;
		result->column_names = column_names;
		result->column_types = column_types;
		return std::move(result);
	}

	string connection_id;
	QuackUri server_uri;
	string table_name;
	vector<string> column_names;
	vector<LogicalType> column_types;
	vector<unique_ptr<DataChunkWrapper>> results;
	bool needs_more_fetch = true;

	unique_ptr<QuackClient> TryGetInitialClient() const {
		lock_guard<mutex> guard(lock);
		return std::move(initial_client);
	}
	void SetInitialClient(unique_ptr<QuackClient> initial_client_p) {
		lock_guard<mutex> guard(lock);
		initial_client = std::move(initial_client_p);
	}

private:
	mutable mutex lock;
	mutable unique_ptr<QuackClient> initial_client;
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
