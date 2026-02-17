#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"

#include "rpc_extension.hpp"
#include "rpc_scan_function.hpp"
#include "rpc_start_function.hpp"
#include "rpc_storage_extension.hpp"

#include "server.hpp"
#include "client.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/storage/storage_extension.hpp"

#define ASIO_STANDALONE // no boost!!

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	loader.RegisterFunction(RpcScanFunction::GetFunction());
	loader.RegisterFunction(RpcStartFunction::GetFunction());

	// (ab)use storage extension info to store our state
	auto ext = duckdb::make_shared_ptr<StorageExtension>();
	ext->storage_info = duckdb::make_uniq<RcpcStorageExtensionInfo>();
	StorageExtension::Register(loader.GetDatabaseInstance().config, RcpcStorageExtensionInfo::STORAGE_EXTENSION_KEY,
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
