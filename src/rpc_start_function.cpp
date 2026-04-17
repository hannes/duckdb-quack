#include "rpc_start_function.hpp"
#include "rpc_storage_extension.hpp"
#include "duckdb/main/database.hpp"

#include <sys/stat.h>

using namespace duckdb;

struct RpcStartStopFunctionData : public TableFunctionData {
	RpcStartStopFunctionData() {
	}

	bool finished = false;
	bool auth_is_default = false;
	RpcUri listen_uri;
};

static unique_ptr<FunctionData> RpcStartBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<RpcStartStopFunctionData>();
	auto &uri_value = input.inputs[0];
	if (uri_value.IsNull() || uri_value.GetValue<string>().empty()) {
		throw InvalidInputException("Invalid listen string specified");
	}
	auto disable_ssl = input.named_parameters.find("disable_ssl") != input.named_parameters.end() &&
	                   input.named_parameters["disable_ssl"].GetValue<bool>();
	bind_data->listen_uri = RpcUri(uri_value.GetValue<string>(), !disable_ssl);

	return_types.emplace_back(LogicalType::VARCHAR);
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("listen_uri");
	names.emplace_back("listen_url");

	auto &config = DBConfig::GetConfig(context);
	Value default_auth_val;
	auto lookup_result_token = config.TryGetCurrentSetting("rpc_authentication_function", default_auth_val);
	bind_data->auth_is_default =
	    lookup_result_token && !default_auth_val.IsNull() && default_auth_val.GetValue<string>() == "rpc_auth_token";

	if (bind_data->auth_is_default) {
		return_types.emplace_back(LogicalType::VARCHAR);
		names.emplace_back("auth_token");
	}

	return std::move(bind_data);
}

static void RpcStartFun(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->CastNoConst<RpcStartStopFunctionData>();
	if (bind_data.finished) {
		return;
	}

	RpcStorageExtensionInfo::GetState(*context.db).FindOrCreateServer(context, bind_data.listen_uri);
	output.SetValue(0, 0, bind_data.listen_uri.Uri());
	output.SetValue(1, 0, bind_data.listen_uri.Http());

	// generate default token if not set
	if (bind_data.auth_is_default) {
		Value default_token_val;
		auto &config = DBConfig::GetConfig(context);

		// TODO there could be a race condition here, lock this
		auto lookup_result_token = config.TryGetCurrentSetting("rpc_default_token", default_token_val);
		D_ASSERT(lookup_result_token);

		if (default_token_val.IsNull()) {
			config.SetOptionByName("rpc_default_token", Value(RpcServer::GenerateSessionId()));
		}

		lookup_result_token = config.TryGetCurrentSetting("rpc_default_token", default_token_val);
		D_ASSERT(lookup_result_token);
		D_ASSERT(!default_token_val.IsNull());
		D_ASSERT(default_token_val.type().id() == LogicalTypeId::VARCHAR);
		output.SetValue(2, 0, default_token_val.GetValue<string>());
	}

	output.SetCardinality(1);
	bind_data.finished = true;
}

TableFunction RpcStartFunction::GetFunction() {
	// TODO add a named parameter to specify where the keys are
	auto fun = TableFunction("rpc_start", {LogicalType::VARCHAR}, RpcStartFun, RpcStartBind);
	fun.named_parameters["disable_ssl"] = LogicalType::BOOLEAN;
	return fun;
}

static unique_ptr<FunctionData> RpcStopBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<RpcStartStopFunctionData>();
	auto &uri_value = input.inputs[0];
	if (uri_value.IsNull() || uri_value.GetValue<string>().empty()) {
		throw InvalidInputException("Invalid listen string specified");
	}
	bind_data->listen_uri =
	    RpcUri(uri_value.GetValue<string>(), /* not really but we don't want to ask the user again */ true);
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("status");

	return std::move(bind_data);
}

static void RpcStopFun(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->CastNoConst<RpcStartStopFunctionData>();
	if (bind_data.finished) {
		return;
	}
	auto &rpc_state = RpcStorageExtensionInfo::GetState(*context.db);
	if (rpc_state.StopServer(context, bind_data.listen_uri)) {
		output.data[0].SetValue(0, StringUtil::Format("Stopped listening on %s", bind_data.listen_uri.Uri()));
	} else {
		output.data[0].SetValue(0, StringUtil::Format("No server found listening on %s", bind_data.listen_uri.Uri()));
	}
	output.SetCardinality(1);
	bind_data.finished = true;
}

TableFunction RpcStopFunction::GetFunction() {
	return TableFunction("rpc_stop", {LogicalType::VARCHAR}, RpcStopFun, RpcStopBind);
}

struct RpcGenerateKeysFunctionData : public TableFunctionData {
	RpcGenerateKeysFunctionData() {
	}

	bool finished = false;
};

static unique_ptr<FunctionData> RpcGenerateKeysBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<RpcGenerateKeysFunctionData>();
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("Status");
	return std::move(result);
}
