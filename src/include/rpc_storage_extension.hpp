#pragma once

#include "duckdb/storage/storage_extension.hpp"
#include "server.hpp"
namespace duckdb {
class DatabaseInstance;

class RcpcStorageExtensionInfo : public StorageExtensionInfo {
public:
	static RcpcStorageExtensionInfo &GetState(const DatabaseInstance &instance);

	RpcServer &FindOrCreateServer(ClientContext &context, const std::string &listen_string);
	static constexpr const char *STORAGE_EXTENSION_KEY = "rpc";

private:
	std::mutex servers_mutex;
	unordered_map<string, unique_ptr<RpcServer>> servers;
};
} // namespace duckdb
