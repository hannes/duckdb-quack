#define DUCKDB_EXTENSION_MAIN

#include "rpc_extension.hpp"
#include "duckdb.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#define ASIO_STANDALONE // no boost!

#include "message.hpp"
#include "server.hpp"
#include "client.hpp"

typedef websocketpp::client<websocketpp::config::asio_tls_client> client;
typedef websocketpp::config::asio_client::message_type::ptr message_ptr;
typedef websocketpp::lib::shared_ptr<websocketpp::lib::asio::ssl::context> context_ptr;

using websocketpp::lib::bind;
using websocketpp::lib::placeholders::_1;
using websocketpp::lib::placeholders::_2;

namespace duckdb {

static unique_ptr<RpcServer> rpc_server; // TODO evil global

inline void RpcScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.AllConstant());
	auto port = args.GetValue(0, 0).GetValue<uint32_t>();
	if (rpc_server) {
		return;
	}
	rpc_server = make_uniq<RpcServer>(state.GetContext());
	rpc_server->Listen(port);
	result.SetValue(0, StringUtil::Format("Listening on port %d", port));
}

struct RpcTableBindData : FunctionData {
	explicit RpcTableBindData() {
	}

	unique_ptr<RpcClient> client;
	string query;

	bool Equals(const FunctionData &other_p) const override {
		throw NotImplementedException("Equals not implemented");
	}

	unique_ptr<FunctionData> Copy() const override {
		throw NotImplementedException("Copy not implemented");
	}
};

// FIXME add some function data
static bool did_execute = false;
static bool done = false;

static unique_ptr<FunctionData> RpcTableBindFun(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	// Set logging to be pretty verbose (everything except message payloads)
	auto uri = input.inputs[0].GetValue<string>();
	auto query = input.inputs[1].GetValue<string>();
	auto client =
	    make_uniq<RpcClient>(uri, StringUtil::StartsWith(StringUtil::Lower(uri), "wss://") ? WEB_SOCKET : UNIX_SOCKET);

	client->Schedule(make_uniq<BindRequestMessage>(query));

	auto bind_response = client->WaitForMessageType<BindResponseMessage>();
	return_types = bind_response->Types();
	names = bind_response->Names();

	auto res = make_uniq<RpcTableBindData>();
	res->client = std::move(client);
	res->query = query;
	did_execute = false;

	return res;
}

static void RpcTableFun(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->Cast<RpcTableBindData>();
	auto &client = *bind_data.client;

	if (!did_execute) {
		client.Send(make_uniq<ExecuteRequestMessage>(bind_data.query));
		// TODO we don't do anything with this>
		client.WaitForMessageType<ExecuteResponseMessage>();
		// now we do the first fetch
		client.Send(make_uniq<FetchRequestMessage>());
		did_execute = true;
		done = false;
	}

	if (!done) {
		auto fetch_response = client.WaitForMessageType<FetchResponseMessage>();
		if (fetch_response->ResponseData() && fetch_response->ResponseData()->size() > 0) {
			output.Reference(*fetch_response->ResponseData());
			output.SetCardinality(fetch_response->ResponseData()->size());
		} else {
			done = true;
		}
	}
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto rpc_scalar_function =
	    ScalarFunction("start_rpc_server", {LogicalType::UINTEGER}, LogicalType::VARCHAR, RpcScalarFun);
	rpc_scalar_function.stability = FunctionStability::VOLATILE;
	loader.RegisterFunction(rpc_scalar_function);

	auto rpc_table_function =
	    TableFunction("call_rpc_server", {LogicalType::VARCHAR, LogicalType::VARCHAR}, RpcTableFun, RpcTableBindFun);
	loader.RegisterFunction(rpc_table_function);
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
