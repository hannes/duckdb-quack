#include "duckdb/main/database.hpp"

#include "quack_startstop.hpp"
#include "quack_storage.hpp"

using namespace duckdb;

struct QuackStartStopFunctionData : public TableFunctionData {
	QuackStartStopFunctionData() {
	}

	bool finished = false;
	QuackUri listen_uri;
	string token;
};

static unique_ptr<FunctionData> QuackServeBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<QuackStartStopFunctionData>();
	auto &uri_value = input.inputs[0];
	if (uri_value.IsNull() || uri_value.GetValue<string>().empty()) {
		throw InvalidInputException("Invalid listen string specified");
	}

	auto allow_other_hostname = input.named_parameters.find("allow_other_hostname") != input.named_parameters.end() &&
	                            input.named_parameters["allow_other_hostname"].GetValue<bool>();

	bind_data->listen_uri =
	    QuackUri(uri_value.GetValue<string>(), /* the server will always listen without SSL */ false);
	if (!allow_other_hostname && !bind_data->listen_uri.IsLocal()) {
		throw InvalidInputException(
		    "Only localhost is allowed as a Quack RPC hostname by default, set allow_other_hostname=true to override. "
		    "We strongly recommend reverse-proxying the Quack RPC when making it publicly available.");
	}

	return_types.emplace_back(LogicalType::VARCHAR);
	return_types.emplace_back(LogicalType::VARCHAR);
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("listen_uri");
	names.emplace_back("listen_url");
	names.emplace_back("auth_token");

	// Every server has a token: either user-supplied or auto-generated. The
	// authn callback (default token-check or a user-defined function) decides
	// what to do with it; the server itself doesn't care which path is in use.
	if (input.named_parameters.find("token") != input.named_parameters.end()) {
		bind_data->token = input.named_parameters["token"].GetValue<string>();
	} else {
		bind_data->token = QuackServer::GenerateRandomToken(*context.db);
	}
	// Validate at bind-time: a length error here fails before the listener
	// thread is spawned, instead of leaving a half-built server behind.
	QuackServer::ValidateToken(bind_data->token);

	return std::move(bind_data);
}

static void QuackServe(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->CastNoConst<QuackStartStopFunctionData>();
	if (bind_data.finished) {
		return;
	}

	auto &server =
	    QuackStorageExtensionInfo::GetState(*context.db).CreateServer(context, bind_data.listen_uri, bind_data.token);
	output.SetValue(0, 0, bind_data.listen_uri.Uri());
	output.SetValue(1, 0, bind_data.listen_uri.Http());
	output.SetValue(2, 0, bind_data.token);

	output.SetCardinality(1);
	bind_data.finished = true;
}

TableFunction QuackServeFunction::GetFunction() {
	auto fun = TableFunction("quack_serve", {LogicalType::VARCHAR}, QuackServe, QuackServeBind);
	fun.named_parameters["disable_ssl"] = LogicalType::BOOLEAN;
	fun.named_parameters["allow_other_hostname"] = LogicalType::BOOLEAN;
	fun.named_parameters["token"] = LogicalType::VARCHAR;

	return fun;
}

static unique_ptr<FunctionData> QuackStopBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<QuackStartStopFunctionData>();
	auto &uri_value = input.inputs[0];
	if (uri_value.IsNull() || uri_value.GetValue<string>().empty()) {
		throw InvalidInputException("Invalid listen string specified");
	}
	bind_data->listen_uri =
	    QuackUri(uri_value.GetValue<string>(), /* not really, but we don't want to ask the user again */ true);
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("status");

	return std::move(bind_data);
}

static void QuackStop(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->CastNoConst<QuackStartStopFunctionData>();
	if (bind_data.finished) {
		return;
	}
	auto &state = QuackStorageExtensionInfo::GetState(*context.db);
	if (state.StopServer(context, bind_data.listen_uri)) {
		output.data[0].SetValue(0, StringUtil::Format("Stopped listening on %s", bind_data.listen_uri.Uri()));
	} else {
		output.data[0].SetValue(0, StringUtil::Format("No server found listening on %s", bind_data.listen_uri.Uri()));
	}
	output.SetCardinality(1);
	bind_data.finished = true;
}

TableFunction QuackStopFunction::GetFunction() {
	return TableFunction("quack_stop", {LogicalType::VARCHAR}, QuackStop, QuackStopBind);
}
