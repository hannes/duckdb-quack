#define DUCKDB_EXTENSION_MAIN

#include "rpc_extension.hpp"
#include "duckdb.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/storage/storage_extension.hpp"

#define ASIO_STANDALONE // no boost!!

#include "message.hpp"
#include "server.hpp"
#include "client.hpp"

namespace duckdb {

const static std::string STORAGE_EXTENSION_KEY = "rpc";

class RcpcStorageExtensionInfo : public StorageExtensionInfo {
public:
	static RcpcStorageExtensionInfo &GetState(const DatabaseInstance &instance) {
		auto &config = instance.config;
		auto ext = StorageExtension::Find(config, STORAGE_EXTENSION_KEY);
		if (!ext) {
			throw std::runtime_error("Fatal error: couldn't find rpc extension state.");
		}
		return *static_cast<RcpcStorageExtensionInfo *>(ext->storage_info.get());
	}

	RpcServer &FindOrCreateServer(ClientContext &context, const std::string &listen_string) {
		std::lock_guard<std::mutex> lock(servers_mutex);
		auto it = servers.find(listen_string);
		if (it != servers.end()) {
			return *it->second;
		}
		auto server = make_uniq<RpcServer>(context);
		server->Listen(listen_string);
		servers.emplace(listen_string, std::move(server));
		return *servers[listen_string];
	}

private:
	std::mutex servers_mutex;
	unordered_map<string, unique_ptr<RpcServer>> servers;
};

inline void RpcScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.AllConstant());
	auto listen_string = args.GetValue(0, 0).GetValue<string>();
	if (listen_string.empty()) {
		throw InvalidInputException("Empty listen string specified");
	}
	auto &rpc_state = RcpcStorageExtensionInfo::GetState(*state.GetContext().db);
	rpc_state.FindOrCreateServer(state.GetContext(), listen_string);
	result.SetValue(0, StringUtil::Format("Listening on %s", listen_string));
}

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
};

static unique_ptr<NodeStatistics> RpcCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<RpcBindData>();
	// TODO how do we get ein estimate here? we could get the explain output on the other side??
	return make_uniq<NodeStatistics>(10000000);
}

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

static void LoadInternal(ExtensionLoader &loader) {
	auto rpc_scalar_function = ScalarFunction("rpc_start", {LogicalType::VARCHAR}, LogicalType::VARCHAR, RpcScalarFun);
	rpc_scalar_function.stability = FunctionStability::VOLATILE;
	loader.RegisterFunction(rpc_scalar_function);

	auto rpc_table_function = TableFunction("rpc_call", {LogicalType::VARCHAR, LogicalType::VARCHAR}, RpcScan, RpcBind,
	                                        RpcInitGlobal, RpcInitLocal);
	rpc_table_function.cardinality = RpcCardinality;

	loader.RegisterFunction(rpc_table_function);

	// (ab)use storage extension info to store our state
	auto ext = duckdb::make_shared_ptr<duckdb::StorageExtension>();
	ext->storage_info = duckdb::make_uniq<RcpcStorageExtensionInfo>();
	StorageExtension::Register(loader.GetDatabaseInstance().config, STORAGE_EXTENSION_KEY, ext);
}

void RpcExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string RpcExtension::Name() {
	return "rpc";
}

std::string RpcExtension::Version() const {
#ifdef EXT_VERSION_RPC
	return EXT_VERSION_RPC;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(rpc, loader) {
	LoadInternal(loader);
}
}
