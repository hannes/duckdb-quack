#pragma once

#define ASIO_STANDALONE // no boost!

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"

#include <thread>

#include <sys/un.h>
//
// // TODO don't like those includes here...
// #include "websocketpp/config/asio.hpp"
// #include "websocketpp/config/asio_client.hpp"
// #include "websocketpp/server.hpp"

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.hpp"

namespace duckdb {

class ClientContext;
class ProtocolMessage;
class Connection;
class MemoryStream;
class QueryResult;
class DatabaseInstance;
class PreparedStatement;

struct RpcConnection {
	mutex lock;
	unique_ptr<Connection> duckdb_connection;
	//	unordered_map<string, std::pair<unique_ptr<PreparedStatement>, unique_ptr<QueryResult>>> duckdb_statements;
	unique_ptr<QueryResult> duckdb_query_result;
};

class RpcServer {
public:
	explicit RpcServer(ClientContext &context_p);
	// TODO should listen be part of the constructor?
	virtual void Listen(const string &listen_string) {};

	optional_ptr<RpcConnection> GetConnection(const string &connection_id);
	string CreateNewConnection(const string &session_id);
	// TODO need something to destroy connections

	virtual ~RpcServer();

protected:
	unique_ptr<ProtocolMessage> HandleMessage(ProtocolMessage &received_message);

protected:
	std::vector<std::thread> listen_threads;

	shared_ptr<DatabaseInstance> db;
	mutex active_connections_mutex;
	unordered_map<string, unique_ptr<RpcConnection>> active_connections;
};

class UnixSocketRpcServer : public RpcServer {
public:
	UnixSocketRpcServer(ClientContext &context_p) : RpcServer(context_p) {
	}

	void Listen(const string &listen_string) override;

	~UnixSocketRpcServer() override;

private:
	static void UnixSocketListenThread(UnixSocketRpcServer *rpc_server);

	int unix_socket_server_fd;
	sockaddr_un unix_socket_address;
	bool unix_socket_keep_listening;
};

class HttpsRpcServer : public RpcServer {
public:
	HttpsRpcServer(ClientContext &context_p) : RpcServer(context_p) {
	}
	void Listen(const string &listen_string) override;

	~HttpsRpcServer() override;

private:
	static void ListenThread(HttpsRpcServer *rpc_server, const string &listen_host, int listen_port);

	unique_ptr<duckdb_httplib_openssl::Server> server;
};
;

} // namespace duckdb
