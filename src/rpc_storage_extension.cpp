#include "rpc_storage_extension.hpp"
#include "duckdb/main/database.hpp"
#include "server.hpp"

using namespace duckdb;

RpcStorageExtensionInfo &RpcStorageExtensionInfo::GetState(const DatabaseInstance &instance) {
	auto &config = instance.config;
	auto ext = StorageExtension::Find(config, STORAGE_EXTENSION_KEY);
	if (!ext) {
		throw std::runtime_error("Fatal error: couldn't find rpc extension state.");
	}
	return *static_cast<RpcStorageExtensionInfo *>(ext->storage_info.get());
}

RpcServer &RpcStorageExtensionInfo::FindOrCreateServer(ClientContext &context, const std::string &listen_string) {
	std::lock_guard<std::mutex> lock(servers_mutex);
	auto it = servers.find(listen_string);
	if (it != servers.end()) {
		return *it->second;
	}
	auto server = make_uniq<RpcServer>(context);
	server->Listen(listen_string);
	servers.emplace(listen_string, std::move(server));
	return *servers[listen_string];
}
