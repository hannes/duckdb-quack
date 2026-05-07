#pragma once

#include "duckdb/storage/storage_extension.hpp"

#include "quack_server.hpp"

namespace duckdb {

class DatabaseInstance;

class QuackStorageExtension : public StorageExtension {
public:
	QuackStorageExtension();
};

class QuackStorageExtensionInfo : public StorageExtensionInfo {
public:
	static QuackStorageExtensionInfo &GetState(const DatabaseInstance &instance);

	QuackServer &CreateServer(ClientContext &context, const QuackUri &listen_uri, const string &token);
	bool StopServer(ClientContext &context, const QuackUri &listen_uri);

	static constexpr const char *STORAGE_EXTENSION_KEY = "quack";

private:
	std::mutex servers_mutex;
	unordered_map<string, unique_ptr<QuackServer>> servers;
};
} // namespace duckdb
