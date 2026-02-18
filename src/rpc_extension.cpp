#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"

#include "rpc_extension.hpp"
#include "rpc_scan_function.hpp"
#include "rpc_start_function.hpp"
#include "rpc_storage_extension.hpp"

#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	loader.RegisterFunction(RpcScanFunction::GetFunction());
	loader.RegisterFunction(RpcStartFunction::GetFunction());
	loader.RegisterFunction(RpcStopFunction::GetFunction());

	loader.RegisterFunction(RpcGenerateKeysFunction::GetFunction());

	// (ab)use storage extension info to store our state
	auto ext = duckdb::make_shared_ptr<StorageExtension>();
	ext->storage_info = duckdb::make_uniq<RpcStorageExtensionInfo>();
	StorageExtension::Register(loader.GetDatabaseInstance().config, RpcStorageExtensionInfo::STORAGE_EXTENSION_KEY,
	                           ext);
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
