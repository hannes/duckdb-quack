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
		std::lock_guard<std::mutex> lock(state_mutex);
		auto it = servers.find(listen_string);
		if (it != servers.end()) {
			return *it->second;
		}
		auto server = make_uniq<RpcServer>(context);
		server->Listen(listen_string);
		servers.emplace(listen_string, std::move(server));
		return *servers[listen_string];
	}

	RpcClient &FindOrCreateClient(ClientContext &context, const std::string &server_address) {
		std::lock_guard<std::mutex> lock(state_mutex);
		auto client_key = server_address + '#' + to_string(context.GetConnectionId());
		auto it = clients.find(client_key);
		if (it != clients.end()) {
			return *it->second;
		}
		auto client = make_uniq<RpcClient>(server_address);
		clients.emplace(client_key, std::move(client));
		return *clients[client_key];
	}

private:
	std::mutex state_mutex;
	unordered_map<string, unique_ptr<RpcServer>> servers;
	unordered_map<string, unique_ptr<RpcClient>> clients;
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

struct RpcTableBindData : FunctionData {
	explicit RpcTableBindData(const string &connection_id_p, RpcClient &client_p)
	    : connection_id(connection_id_p), client(client_p) {
	}
	string connection_id;

	RpcClient &client;

	bool Equals(const FunctionData &other_p) const override {
		throw NotImplementedException("Equals not implemented");
	}

	unique_ptr<FunctionData> Copy() const override {
		throw NotImplementedException("Copy not implemented");
	}
};

static unique_ptr<FunctionData> RpcTableBindFun(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	// Set logging to be pretty verbose (everything except message payloads)
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("call_rpc_server URI and query parameters cannot be NULL");
	}

	auto uri = input.inputs[0].GetValue<string>();
	auto query = input.inputs[1].GetValue<string>();

	auto &client = RcpcStorageExtensionInfo::GetState(*context.db).FindOrCreateClient(context, uri);

	auto connection_request_response =
	    client.MakeRequest<ConnectionResponseMessage>(make_uniq<ConnectionRequestMessage>());

	auto bind_data = make_uniq<RpcTableBindData>(connection_request_response->ConnectionId(), client);

	auto bind_response =
	    client.MakeRequest<PrepareResponseMessage>(make_uniq<PrepareRequestMessage>(bind_data->connection_id, query));

	return_types = bind_response->Types();
	names = bind_response->Names();

	return bind_data;
}

struct RpcTableFunctionState : GlobalTableFunctionState {
	bool done = false;
};

static unique_ptr<GlobalTableFunctionState> RpcTableFunInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<RpcTableFunctionState>();
}

static void RpcTableFun(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->Cast<RpcTableBindData>();
	auto &function_state = input.global_state->Cast<RpcTableFunctionState>();

	if (!function_state.done) {
		auto fetch_response =
		    bind_data.client.MakeRequest<FetchResponseMessage>(make_uniq<FetchRequestMessage>(bind_data.connection_id));
		if (fetch_response->ResponseData() && fetch_response->ResponseData()->size() > 0) {
			output.Reference(*fetch_response->ResponseData());
			output.SetCardinality(fetch_response->ResponseData()->size());
		} else {
			function_state.done = true;
		}
	}
}

static void LoadInternal(ExtensionLoader &loader) {
	auto rpc_scalar_function =
	    ScalarFunction("start_rpc_server", {LogicalType::VARCHAR}, LogicalType::VARCHAR, RpcScalarFun);
	rpc_scalar_function.stability = FunctionStability::VOLATILE;
	loader.RegisterFunction(rpc_scalar_function);

	auto rpc_table_function = TableFunction("call_rpc_server", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                        RpcTableFun, RpcTableBindFun, RpcTableFunInit);
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
