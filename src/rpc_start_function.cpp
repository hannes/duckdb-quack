#include "rpc_start_function.hpp"
#include "rpc_storage_extension.hpp"
#include "duckdb/function/scalar_function.hpp"

using namespace duckdb;

static void RpcScalarFun(const DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.AllConstant());
	auto listen_value = args.GetValue(0, 0);
	auto listen_string = listen_value.GetValue<string>();
	if (listen_value.IsNull() || listen_string.empty()) {
		throw InvalidInputException("Invalid listen string specified");
	}
	auto &rpc_state = RpcStorageExtensionInfo::GetState(*state.GetContext().db);
	rpc_state.FindOrCreateServer(state.GetContext(), listen_string);
	result.SetValue(0, StringUtil::Format("Listening on %s", listen_string));
}

ScalarFunction RpcStartFunction::GetFunction() {
	auto rpc_start_function = ScalarFunction("rpc_start", {LogicalType::VARCHAR}, LogicalType::VARCHAR, RpcScalarFun);
	rpc_start_function.stability = FunctionStability::VOLATILE;
	rpc_start_function.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return rpc_start_function;
}
