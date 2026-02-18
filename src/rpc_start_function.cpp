#include "rpc_start_function.hpp"
#include "rpc_storage_extension.hpp"
#include "ssl_key_generator.hpp"

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/database.hpp"

using namespace duckdb;

struct RpcStartStopFunctionData : public TableFunctionData {
	RpcStartStopFunctionData() {
	}

	bool finished = false;
	string listen_string;
};

static unique_ptr<FunctionData> RpcStartStopBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<RpcStartStopFunctionData>();
	auto &uri_value = input.inputs[0];
	if (uri_value.IsNull() || uri_value.GetValue<string>().empty()) {
		throw InvalidInputException("Invalid listen string specified");
	}
	result->listen_string = input.inputs[0].GetValue<string>();
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("Status");
	return std::move(result);
}

static void RpcStartFun(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->CastNoConst<RpcStartStopFunctionData>();
	if (bind_data.finished) {
		return;
	}
	auto &rpc_state = RpcStorageExtensionInfo::GetState(*context.db);
	rpc_state.FindOrCreateServer(context, bind_data.listen_string);
	output.data[0].SetValue(0, StringUtil::Format("Listening on %s", bind_data.listen_string));
	output.SetCardinality(1);
	bind_data.finished = true;
}

TableFunction RpcStartFunction::GetFunction() {
	// TODO add a named parameter to specify where the keys are
	return TableFunction("rpc_start", {LogicalType::VARCHAR}, RpcStartFun, RpcStartStopBind);
}

static void RpcStopFun(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->CastNoConst<RpcStartStopFunctionData>();
	if (bind_data.finished) {
		return;
	}
	auto &rpc_state = RpcStorageExtensionInfo::GetState(*context.db);
	if (rpc_state.StopServer(context, bind_data.listen_string)) {
		output.data[0].SetValue(0, StringUtil::Format("Stopped listening on %s", bind_data.listen_string));
	} else {
		output.data[0].SetValue(0, StringUtil::Format("No server found listening on %s", bind_data.listen_string));
	}
	output.SetCardinality(1);
	bind_data.finished = true;
}

TableFunction RpcStopFunction::GetFunction() {
	return TableFunction("rpc_stop", {LogicalType::VARCHAR}, RpcStopFun, RpcStartStopBind);
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

static void RpcGenerateKeysFun(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->CastNoConst<RpcGenerateKeysFunctionData>();
	if (bind_data.finished) {
		return;
	}

	output.SetCardinality(1);
	bind_data.finished = true;

	auto &fs = FileSystem::GetFileSystem(context);

	auto certificate_directory = SslKeyGenerator::GetDefaultCertificateDirectory(fs);

	auto server_key_file = fs.JoinPath(certificate_directory, "server.pem");
	auto private_key_file = fs.JoinPath(certificate_directory, "private_key.pem");
	auto dh_param_file = fs.JoinPath(certificate_directory, "dh.pem");

	output.SetCardinality(1);
	bind_data.finished = true;
	if (fs.FileExists(server_key_file) && fs.FileExists(private_key_file) && fs.FileExists(dh_param_file)) {
		output.data[0].SetValue(
		    0, StringUtil::Format("Key files exist in %s - remove to recreate them", certificate_directory));
		return;
	}
	SslKeyGenerator::GenerateSslKeys(server_key_file, private_key_file, dh_param_file, 3650);
	output.data[0].SetValue(0, StringUtil::Format("Key files generated in %s", certificate_directory));
}

TableFunction RpcGenerateKeysFunction::GetFunction() {
	// TODO add a named parameter to specify where the keys are
	return TableFunction("rpc_generate_keys", {}, RpcGenerateKeysFun, RpcGenerateKeysBind);
}
