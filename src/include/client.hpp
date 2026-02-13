#pragma once

#include "message.hpp"

#define ASIO_STANDALONE // no boost!

#include "websocketpp/client.hpp"
#include "websocketpp/config/asio.hpp"
#include "websocketpp/config/asio_client.hpp"

namespace duckdb {

typedef websocketpp::config::asio_client::message_type::ptr message_ptr;
typedef websocketpp::lib::shared_ptr<websocketpp::lib::asio::ssl::context> context_ptr;
typedef websocketpp::client<websocketpp::config::asio_tls_client> client;

enum Mode { WEB_SOCKET, UNIX_SOCKET };

struct RpcClient {
	RpcClient(const string &uri_p);

	~RpcClient();

	void OnOpen(websocketpp::connection_hdl hdl);
	void OnMessage(websocketpp::connection_hdl hdl, message_ptr msg);
	void OnFail(websocketpp::connection_hdl hdl);

	template <class TARGET>
	unique_ptr<TARGET> WaitForMessageType() {
		auto message = WaitForMessage(TARGET::TYPE).release();
		return unique_ptr<TARGET>(reinterpret_cast<TARGET *>(message));
	}

	unique_ptr<ProtocolMessage> WaitForMessage(MessageType expected_type);

	void SendInternal(websocketpp::connection_hdl hdl);
	void Schedule(unique_ptr<ProtocolMessage> message_p);
	void Send(unique_ptr<ProtocolMessage> message_p);

	std::thread conn_thread;
	unique_ptr<ProtocolMessage> message;
	deque<unique_ptr<ProtocolMessage>> messages;
	std::mutex messages_mutex;
	std::condition_variable messages_wait;
	string uri;
	client c;
	client::connection_ptr con;
	Mode mode;
	int unix_socket;
};
} // namespace duckdb
