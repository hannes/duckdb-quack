#pragma once

#define ASIO_STANDALONE // no boost!

#include "duckdb/common/serializer/memory_stream.hpp"
#include "websocketpp/config/asio.hpp"
#include "websocketpp/config/asio_client.hpp"
#include "websocketpp/server.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {

typedef websocketpp::server<websocketpp::config::asio_tls> server;
typedef websocketpp::config::asio_client::message_type::ptr message_ptr;

enum tls_mode { MOZILLA_INTERMEDIATE = 1, MOZILLA_MODERN = 2 };

class ClientContext;
class ProtocolMessage;

struct RpcServer {
	RpcServer(ClientContext &context_p);

	void Listen(const string &listen_string);

	void OnMessage(websocketpp::connection_hdl hdl, message_ptr msg);
	unique_ptr<ProtocolMessage> HandleMessage(ProtocolMessage &received_message);

	ClientContext &context;

	~RpcServer();

	std::mutex active_connections_mutex;
	struct RpcConnection {
		mutex lock;
		unique_ptr<Connection> duckdb_connection;
		unique_ptr<QueryResult> duckdb_query_result;
	};
	unordered_map<string, unique_ptr<RpcConnection>> active_connections;

	// TODO move this to implementation
	optional_ptr<RpcConnection> GetConnection(const string &connection_id) {
		std::lock_guard<std::mutex> lock(active_connections_mutex);
		auto it = active_connections.find(connection_id);
		if (it != active_connections.end()) {
			return it->second.get();
		}
		throw IOException("Invalid connection id");
	}

	string CreateNewConnection() {
		std::lock_guard<std::mutex> lock(active_connections_mutex);
		// TODO this will need cryptographic randomness I fear
		auto connection_id = StringUtil::GenerateRandomName(40);
		D_ASSERT(active_connections.find(connection_id) == active_connections.end());

		auto new_connection = make_uniq<RpcConnection>();
		new_connection->duckdb_connection = make_uniq<Connection>(*context.db);
		active_connections[connection_id] = std::move(new_connection);
		return connection_id;
	}

	// TODO need something to destroy connections

	std::thread listen_thread;
	std::thread unix_socket_thread;

	server s;

	string listen_string;
};
} // namespace duckdb
