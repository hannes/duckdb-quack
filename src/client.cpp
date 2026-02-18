#include "client.hpp"

using namespace duckdb;

unique_ptr<ProtocolMessage> RpcClient::WaitForMessageInternal(MessageType expected_type) {
	auto result = Receive();
	if (result->Type() != expected_type) {
		if (result->Type() == MessageType::ERROR) {
			throw IOException("Expected %s message, got error message instead: %s", MessageTypeToString(expected_type),
			                  result->Cast<ErrorMessage>().Error().c_str());
		}
		throw IOException("Expected %s message, got %s instead", MessageTypeToString(expected_type),
		                  MessageTypeToString(result->Type()));
	}
	return result;
}

UnixSocketRpcClient::UnixSocketRpcClient(const string &uri_p) : RpcClient(uri_p) {
	D_ASSERT(!StringUtil::StartsWith(uri, "wss://"));

	sockaddr_un remote;
	unix_socket_fd = 0;
	memset(&remote, 0, sizeof(sockaddr_un));

	if ((unix_socket_fd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0) {
		throw IOException("Client: Error on socket() call %s", strerror(errno));
	}
	remote.sun_family = AF_UNIX;
	strcpy(remote.sun_path, uri.c_str());
	auto data_len = SUN_LEN(&remote);

	if (connect(unix_socket_fd, reinterpret_cast<sockaddr *>(&remote), data_len)) {
		throw IOException("Client: Error on connect(%s) call: %s", uri, strerror(errno));
	}
}

UnixSocketRpcClient::~UnixSocketRpcClient() {
	close(unix_socket_fd);
}

unique_ptr<ProtocolMessage> UnixSocketRpcClient::Receive() {
	return ProtocolMessage::FromSocket(unix_socket_fd, read_stream);
}

void UnixSocketRpcClient::Send(unique_ptr<ProtocolMessage> message) {
	D_ASSERT(message);
	message->ToSocket(unix_socket_fd, write_stream);
}

using websocketpp::lib::bind;
using websocketpp::lib::placeholders::_1;
using websocketpp::lib::placeholders::_2;

static context_ptr on_tls_init_client(const char *, websocketpp::connection_hdl) {
	context_ptr ctx = websocketpp::lib::make_shared<asio::ssl::context>(asio::ssl::context::sslv23);
	try {
		ctx->set_options(asio::ssl::context::default_workarounds | asio::ssl::context::no_sslv2 |
		                 asio::ssl::context::no_sslv3 | asio::ssl::context::single_dh_use);
		ctx->set_verify_mode(asio::ssl::verify_none);
	} catch (std::exception &e) {
		throw InternalException(e.what());
	}
	return ctx;
}

WebSocketRpcClient::WebSocketRpcClient(const string &uri_p) : RpcClient(uri_p) {
	D_ASSERT(StringUtil::StartsWith(uri, "wss://"));

	// some ugly setup
	websocket_client.set_access_channels(websocketpp::log::alevel::none);
	websocket_client.set_error_channels(websocketpp::log::alevel::none);
	websocket_client.init_asio();
	websocket_client.set_tls_init_handler(bind(&on_tls_init_client, "localhost", ::_1));
	websocket_client.set_user_agent("DuckDB");
	websocket_client.set_message_handler(bind(&WebSocketRpcClient::OnMessage, this, ::_1, ::_2));
	websocket_client.set_fail_handler(bind(&WebSocketRpcClient::OnFail, this, ::_1));
	websocket_client.set_open_handler(bind(&WebSocketRpcClient::OnOpen, this, ::_1));

	websocketpp::lib::error_code ec;
	websocket_connection = websocket_client.get_connection(uri, ec);
	if (ec) {
		throw IOException(ec.message());
	}
	websocket_client.connect(websocket_connection);

	// launch ze background thread to listen
	websocket_listen_thread = std::thread([=]() { websocket_client.run(); });
}

WebSocketRpcClient::~WebSocketRpcClient() {
	if (websocket_connection && websocket_exception.empty()) {
		websocket_connection->close(websocketpp::close::status::normal, "");
	}
	websocket_listen_thread.join();
}

void WebSocketRpcClient::OnOpen(websocketpp::connection_hdl hdl_p) {
	connection_open = true;
}

void WebSocketRpcClient::OnMessage(const websocketpp::connection_hdl &hdl, const message_ptr msg) {
	auto &payload = msg->get_payload();
	MemoryStream wss_read_stream((data_ptr_t)payload.data(), payload.size());
	auto received_message = ProtocolMessage::FromMemoryStream(wss_read_stream);
	std::unique_lock<std::mutex> lock(messages_mutex);
	messages.push_front(std::move(received_message));
	messages_wait.notify_one();
}

// boo
void WebSocketRpcClient::OnFail(websocketpp::connection_hdl hdl) {
	client::connection_ptr con = websocket_client.get_con_from_hdl(hdl);
	// there is more error stuff to expose here if required
	websocket_exception = StringUtil::Format("RPC request to %s failed: %s", uri, con->get_ec().message());
}

unique_ptr<ProtocolMessage> WebSocketRpcClient::Receive() {
	if (!websocket_exception.empty()) {
		throw IOException(websocket_exception);
	}
	unique_ptr<ProtocolMessage> result;
	std::unique_lock<std::mutex> lock(messages_mutex);
	messages_wait.wait(lock, [=] { return !messages.empty(); });
	result = std::move(messages.back());
	messages.pop_back();
	return result;
}

void WebSocketRpcClient::Send(unique_ptr<ProtocolMessage> message_p) {
	D_ASSERT(message_p);
	try {
		// we have to wait till the connection is actually open on connect
		while (!connection_open && websocket_exception.empty()) {
			usleep(10);
		}

		if (!websocket_exception.empty()) {
			throw IOException(websocket_exception);
		}
		message_p->ToMemoryStream(write_stream);
		websocket_client.send(websocket_connection, write_stream.GetData(), write_stream.GetPosition(),
		                      websocketpp::frame::opcode::binary);
	} catch (websocketpp::exception const &e) {
		throw IOException(e.what());
	}
}
