#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"

#include "rpc_extension.hpp"
#include "rpc_scan_function.hpp"
#include "rpc_start_function.hpp"
#include "rpc_storage_extension.hpp"

#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/catalog/default/default_table_functions.hpp"

#include "catalog.hpp"

namespace duckdb {

static unique_ptr<Catalog> RpcAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                     AttachedDatabase &db, const string &name, AttachInfo &info,
                                     AttachOptions &attach_options) {
	return make_uniq<RpcCatalog>(db, info.path);
}

static unique_ptr<TransactionManager> RpcCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                  AttachedDatabase &db, Catalog &catalog) {
	auto &rpc_catalog = catalog.Cast<RpcCatalog>();
	return make_uniq<RpcTransactionManager>(db, rpc_catalog);
}

class RpcStorageExtension : public StorageExtension {
public:
	RpcStorageExtension() {
		attach = RpcAttach;
		create_transaction_manager = RpcCreateTransactionManager;
	}
};

// pass session id
static void RpcAuthToken(DataChunk &args, ExpressionState &, Vector &result) {
	D_ASSERT(args.size() == 2);
	D_ASSERT(args.GetTypes()[0].id() == LogicalTypeId::VARCHAR);
	D_ASSERT(args.GetTypes()[1].id() == LogicalTypeId::VARCHAR);
	D_ASSERT(result.GetType().id() == LogicalTypeId::BOOLEAN);

	auto auth_str = args.GetValue(1, 0).GetValue<string>();
	result.SetValue(0, Value(auth_str == "mellon")); // speak 'friend' and enter
}

// pass session id
static void RpcDummyAuthorization(DataChunk &args, ExpressionState &, Vector &result) {
	D_ASSERT(args.size() == 2);
	D_ASSERT(args.GetTypes()[0].id() == LogicalTypeId::VARCHAR);
	D_ASSERT(args.GetTypes()[1].id() == LogicalTypeId::VARCHAR);
	D_ASSERT(result.GetType().id() == LogicalTypeId::BOOLEAN);

	result.SetValue(0, Value(true)); // choose life
}

static void LoadInternal(ExtensionLoader &loader) {
	loader.SetDescription("Adds support for DuckDB Remote Procedure Calls (RPC)");

	loader.RegisterFunction(RpcScanFunction::GetFunction());
	loader.RegisterFunction(RpcScanByNameFunction::GetFunction());

	loader.RegisterFunction(RpcStartFunction::GetFunction());
	loader.RegisterFunction(RpcStopFunction::GetFunction());
	loader.RegisterFunction(RpcGenerateKeysFunction::GetFunction());

	// the default authentication function
	ScalarFunction rpc_auth_token("rpc_auth_token",
	                              {/* session id */ LogicalType::VARCHAR, /* auth string */ LogicalType::VARCHAR},
	                              LogicalType::BOOLEAN, RpcAuthToken);
	loader.RegisterFunction(rpc_auth_token);

	ScalarFunction rpc_authorization("rpc_dummy_authorization",
	                                 {/* session id */ LogicalType::VARCHAR, /* query string */ LogicalType::VARCHAR},
	                                 LogicalType::BOOLEAN, RpcDummyAuthorization);
	loader.RegisterFunction(rpc_authorization);

	// (ab)use storage extension info to store our state
	auto ext = duckdb::make_shared_ptr<RpcStorageExtension>();
	ext->storage_info = duckdb::make_uniq<RpcStorageExtensionInfo>();
	StorageExtension::Register(loader.GetDatabaseInstance().config, RpcStorageExtensionInfo::STORAGE_EXTENSION_KEY,
	                           ext);

	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.AddExtensionOption("rpc_authentication_function", "Name of a callback function for authentication",
	                          LogicalType::VARCHAR, Value("rpc_auth_token"));
	config.AddExtensionOption("rpc_authorization_function", "Name of a callback function for authorization",
	                          LogicalType::VARCHAR, Value("rpc_dummy_authorization"));
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
