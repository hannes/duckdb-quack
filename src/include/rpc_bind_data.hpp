#include "duckdb/function/table_function.hpp"

namespace duckdb {
class RpcClient;
struct RpcBindData : FunctionData {
	bool Equals(const FunctionData &other_p) const override {
		throw NotImplementedException("Equals not implemented");
	}

	unique_ptr<FunctionData> Copy() const override {
		throw NotImplementedException("Copy not implemented");
	}
	string connection_id;
	string uri;
	optional_idx estimated_cardinality;
	unique_ptr<RpcClient> initial_client;
};
} // namespace duckdb
