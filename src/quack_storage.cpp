#include <thread>

#include "duckdb/main/database.hpp"

#include "quack_storage.hpp"
#include "quack_server.hpp"

using namespace duckdb;

QuackStorageExtensionInfo &QuackStorageExtensionInfo::GetState(const DatabaseInstance &instance) {
	auto &config = instance.config;
	auto ext = StorageExtension::Find(config, STORAGE_EXTENSION_KEY);
	if (!ext) {
		throw std::runtime_error("Fatal error: couldn't find rpc extension state.");
	}
	return *static_cast<QuackStorageExtensionInfo *>(ext->storage_info.get());
}

QuackServer &QuackStorageExtensionInfo::FindOrCreateServer(ClientContext &context, const QuackUri &listen_uri) {
	std::lock_guard<std::mutex> lock(servers_mutex);
	auto it = servers.find(listen_uri.Uri());
	if (it != servers.end()) {
		return *it->second;
	}
	unique_ptr<QuackServer> server;
	server = make_uniq<HttpQuackServer>(context);
	server->Listen(listen_uri);
	servers.emplace(listen_uri.Uri(), std::move(server));
	return *servers[listen_uri.Uri()];
}

bool QuackStorageExtensionInfo::StopServer(ClientContext &context, const QuackUri &listen_uri) {
	unique_ptr<QuackServer> to_destroy;
	{
		std::lock_guard<std::mutex> lock(servers_mutex);
		const auto it = servers.find(listen_uri.Uri());
		if (it == servers.end()) {
			return false;
		}
		to_destroy = std::move(it->second);
		servers.erase(it);
	}
	// Synchronously free the listening port so that clients racing a subsequent
	// connect() after rpc_stop observe a real refusal rather than a stale socket.
	to_destroy->Close();
	// Full destruction (httplib worker-pool join) runs off-thread so that when
	// quack_stop is invoked from inside one of the server's own worker threads
	std::thread([srv = std::move(to_destroy)]() mutable { srv.reset(); }).detach();
	return true;
}
