#pragma once

#define ASIO_STANDALONE // no boost!

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/mutex.hpp"

// TODO don't like those includes here...
#include "websocketpp/config/asio.hpp"
#include "websocketpp/config/asio_client.hpp"
#include "websocketpp/server.hpp"

namespace duckdb {

typedef websocketpp::server<websocketpp::config::asio_tls> server;
typedef websocketpp::config::asio_client::message_type::ptr message_ptr;
typedef websocketpp::lib::shared_ptr<websocketpp::lib::asio::ssl::context> context_ptr;

class ClientContext;
class ProtocolMessage;
class Connection;
class MemoryStream;
class QueryResult;
class DatabaseInstance;

struct RpcConnection {
	mutex lock;
	unique_ptr<Connection> duckdb_connection;
	unique_ptr<QueryResult> duckdb_query_result;
};

class RpcServer {
public:
	explicit RpcServer(ClientContext &context_p);
	void Listen(const string &listen_string); // TODO should listen be part of the constructor?

	optional_ptr<RpcConnection> GetConnection(const string &connection_id);
	string CreateNewConnection();
	// TODO need something to destroy connections

	~RpcServer();

private:
	void OnMessage(const websocketpp::connection_hdl &hdl, const message_ptr &msg);

	unique_ptr<ProtocolMessage> HandleMessage(ProtocolMessage &received_message);

	static context_ptr OnTlsInit(RpcServer *rpc_server, const websocketpp::connection_hdl &hdl);
	static void WebsocketListenThread(RpcServer *rpc_server);
	static void UnixSocketListenThread(RpcServer *rpc_server);

private:
	shared_ptr<DatabaseInstance> db;
	string listen_string;
	mutex active_connections_mutex;
	unordered_map<string, unique_ptr<RpcConnection>> active_connections;
	std::thread listen_thread;

	server websocket_server;
	int unix_socket_server_fd;
	sockaddr_un unix_socket_address;
	bool unix_socket_keep_listening;
};
} // namespace duckdb
