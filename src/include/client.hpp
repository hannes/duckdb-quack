#pragma once

#include "message.hpp"

#define ASIO_STANDALONE // no boost!

#include "websocketpp/client.hpp"
#include "websocketpp/config/asio.hpp"
#include "websocketpp/config/asio_client.hpp"

namespace duckdb {

typedef websocketpp::connection_hdl connection_ptr;
typedef websocketpp::config::asio_client::message_type::ptr message_ptr;
typedef websocketpp::lib::shared_ptr<websocketpp::lib::asio::ssl::context> context_ptr;
typedef websocketpp::client<websocketpp::config::asio_tls_client> client;

enum Mode { WEB_SOCKET, UNIX_SOCKET };

class RpcClient {
public:
	RpcClient(const string &uri_p);
	template <class TARGET>
	unique_ptr<TARGET> MakeRequest(unique_ptr<ProtocolMessage> request_message) {
		//	printf("C SEND %s\n", MessageTypeToString(request_message->Type()).c_str());
		Send(std::move(request_message));
		auto response_message = WaitForMessageInternal(TARGET::TYPE).release();
		//	printf("C RECV %s\n", MessageTypeToString(response_message->Type()).c_str());

		return unique_ptr<TARGET>(reinterpret_cast<TARGET *>(response_message));
	}

	void WebsocketListen();
	~RpcClient();

private:
	void OnOpen(connection_ptr hdl);
	void OnMessage(connection_ptr hdl, message_ptr msg);
	void OnFail(connection_ptr hdl);

	unique_ptr<ProtocolMessage> WaitForMessageInternal(MessageType expected_type);
	void Send(unique_ptr<ProtocolMessage> message_p);

	std::thread conn_thread;
	unique_ptr<ProtocolMessage> message;
	deque<unique_ptr<ProtocolMessage>> messages;
	std::mutex messages_mutex;
	std::condition_variable messages_wait;

	string uri;
	client websocket_client;
	client::connection_ptr websocket_connection;
	bool connection_open = false;
	Mode mode;
	int unix_socket_fd;
};
} // namespace duckdb
