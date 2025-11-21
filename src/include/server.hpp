#pragma once

#define ASIO_STANDALONE // no boost!

#include "websocketpp/config/asio.hpp"
#include "websocketpp/config/asio_client.hpp"
#include "websocketpp/server.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {

typedef websocketpp::server<websocketpp::config::asio_tls> server;
typedef websocketpp::config::asio_client::message_type::ptr message_ptr;
typedef websocketpp::lib::shared_ptr<websocketpp::lib::asio::ssl::context> context_ptr;

enum tls_mode { MOZILLA_INTERMEDIATE = 1, MOZILLA_MODERN = 2 };

class ClientContext;

struct RpcServer {
	RpcServer(ClientContext &context_p);

	void Listen(uint32_t port);

	void OnMessage(websocketpp::connection_hdl hdl, message_ptr msg);

	ClientContext &context;

	// FIXME have one duckdb per ws connection!
	Connection internal_connection;
	unique_ptr<PendingQueryResult> pending_result;

	unique_ptr<QueryResult> query_result;
	std::thread listen_thread;
	server s;
};
} // namespace duckdb
