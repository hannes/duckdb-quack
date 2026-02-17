#include "rpc_scan_function.hpp"
#include "client.hpp"

#include "duckdb/function/table_function.hpp"

using namespace duckdb;

struct RpcBindData : FunctionData {
	unique_ptr<RpcClient> client;
	string connection_id;
	string uri;

	bool Equals(const FunctionData &other_p) const override {
		throw NotImplementedException("Equals not implemented");
	}

	unique_ptr<FunctionData> Copy() const override {
		throw NotImplementedException("Copy not implemented");
	}
};

static unique_ptr<FunctionData> RpcBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
	// Set logging to be pretty verbose (everything except message payloads)
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("call_rpc_server URI and query parameters cannot be NULL");
	}

	auto query = input.inputs[1].GetValue<string>();
	auto bind_data = make_uniq<RpcBindData>();
	bind_data->uri = input.inputs[0].GetValue<string>();
	bind_data->client = make_uniq<RpcClient>(bind_data->uri);

	auto connection_request_response =
	    bind_data->client->MakeRequest<ConnectionResponseMessage>(make_uniq<ConnectionRequestMessage>());
	bind_data->connection_id = connection_request_response->ConnectionId();

	auto bind_response = bind_data->client->MakeRequest<PrepareResponseMessage>(
	    make_uniq<PrepareRequestMessage>(bind_data->connection_id, query));

	return_types = bind_response->Types();
	names = bind_response->Names();

	return bind_data;
}

struct RpcLocalState : public LocalTableFunctionState {
	unique_ptr<RpcClient> client;

	explicit RpcLocalState() {
	}
	~RpcLocalState() {
	}
};

struct RpcGlobalState : GlobalTableFunctionState {
	atomic<bool> done;
	RpcGlobalState() {
		done = false;
	}
	idx_t MaxThreads() const override {
		return MAX_THREADS; // TODO we might want to only use one thread for low-cardinality queries
	}
};

unique_ptr<GlobalTableFunctionState> RpcInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<RpcGlobalState>();
}

unique_ptr<LocalTableFunctionState> RpcInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                 GlobalTableFunctionState *global_state_p) {
	auto &bind_data = input.bind_data->Cast<RpcBindData>();
	auto &global_state = global_state_p->Cast<RpcGlobalState>();
	if (global_state.done) {
		return nullptr; // TODO does this work?
	}
	auto local_state = make_uniq<RpcLocalState>();
	local_state->client = make_uniq<RpcClient>(bind_data.uri);
	// TODO re-use client from bind data for first conn

	return local_state;
}

static void RpcScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->Cast<RpcBindData>();
	auto &global_state = input.global_state->Cast<RpcGlobalState>();
	auto &local_state = input.local_state->Cast<RpcLocalState>();

	if (global_state.done) {
		return;
	}
	auto fetch_response =
	    local_state.client->MakeRequest<FetchResponseMessage>(make_uniq<FetchRequestMessage>(bind_data.connection_id));
	if (!fetch_response->ResponseData() || fetch_response->ResponseData()->size() == 0) {
		global_state.done = true;
		return;
	}

	output.Reference(*fetch_response->ResponseData());
	output.SetCardinality(fetch_response->ResponseData()->size());
}

TableFunction RpcScanFunction::GetFunction() {
	return TableFunction("rpc_call", {LogicalType::VARCHAR, LogicalType::VARCHAR}, RpcScan, RpcBind, RpcInitGlobal,
	                     RpcInitLocal);
}
