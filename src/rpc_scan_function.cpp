#include "rpc_scan_function.hpp"
#include "client.hpp"

#include "duckdb/function/table_function.hpp"

using namespace duckdb;

struct RpcBindData : FunctionData {
	bool Equals(const FunctionData &other_p) const override {
		throw NotImplementedException("Equals not implemented");
	}

	unique_ptr<FunctionData> Copy() const override {
		throw NotImplementedException("Copy not implemented");
	}
	unique_ptr<RpcClient> client;
	string connection_id;
	string uri;
	optional_idx estimated_cardinality;
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

	bind_data->estimated_cardinality = bind_response->EstimatedCardinality();
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
	RpcGlobalState(idx_t max_threads_p) : max_threads(max_threads_p), done(false) {
	}
	idx_t MaxThreads() const override {
		return max_threads;
	}
	idx_t max_threads;
	atomic<bool> done;
};

unique_ptr<GlobalTableFunctionState> RpcInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<RpcBindData>();
	auto num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();

	idx_t max_threads = 1;
	auto min_chunks_per_thread = 10.0; // TODO make this a parameter

	// small heuristic that scales down the number of threads working on retrieving a result set somewhat gracefully.
	if (bind_data.estimated_cardinality.IsValid()) {
		auto target_threads =
		    (bind_data.estimated_cardinality.GetIndex() / STANDARD_VECTOR_SIZE / min_chunks_per_thread) * num_threads;
		if (target_threads > 1) {
			max_threads = target_threads;
		}
		if (target_threads > num_threads) {
			max_threads = GlobalTableFunctionState::MAX_THREADS;
		}
	}

	return make_uniq<RpcGlobalState>(max_threads);
}

unique_ptr<LocalTableFunctionState> RpcInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                 GlobalTableFunctionState *global_state_p) {
	auto &bind_data = input.bind_data->Cast<RpcBindData>();
	auto &global_state = global_state_p->Cast<RpcGlobalState>();
	if (global_state.done) {
		return nullptr;
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
