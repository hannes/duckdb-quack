#include "client.hpp"

using namespace duckdb;

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

void RpcClient::WebsocketListen() {
	websocketpp::lib::error_code ec;
	websocket_connection = websocket_client.get_connection(uri, ec);
	if (ec) {
		throw IOException(ec.message());
	}
	// actually listen and run event loop
	websocket_client.connect(websocket_connection);
	websocket_client.run();
}

static void ConnectionThread(RpcClient *rpc_client) {
	D_ASSERT(rpc_client);
	rpc_client->WebsocketListen();
}

RpcClient::RpcClient(const string &uri_p) : uri(uri_p) {
	if (StringUtil::StartsWith(uri, "wss:")) {
		mode = WEB_SOCKET;
	} else {
		mode = UNIX_SOCKET;
	}

	if (mode == WEB_SOCKET) {
		// some ugly setup
		websocket_client.set_access_channels(websocketpp::log::alevel::none);
		websocket_client.set_error_channels(websocketpp::log::alevel::none);
		websocket_client.init_asio();
		websocket_client.set_tls_init_handler(bind(&on_tls_init_client, "localhost", ::_1));
		websocket_client.set_user_agent("DuckDB");
		websocket_client.set_message_handler(bind(&RpcClient::OnMessage, this, ::_1, ::_2));
		websocket_client.set_fail_handler(bind(&RpcClient::OnFail, this, ::_1));
		websocket_client.set_open_handler(bind(&RpcClient::OnOpen, this, ::_1));

		// launch ze background thread to listen
		conn_thread = std::thread([=]() {
			ConnectionThread(this);
			return 1;
		});
	}
	if (mode == UNIX_SOCKET) {
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
}

RpcClient::~RpcClient() {
	if (mode == WEB_SOCKET) {
		if (websocket_connection) {
			websocket_connection->close(websocketpp::close::status::normal, "");
		}
		conn_thread.join();
	}
	if (mode == UNIX_SOCKET) {
		// TODO check for errors etc.
		close(unix_socket_fd);
	}
}

void RpcClient::OnOpen(websocketpp::connection_hdl hdl_p) {
	connection_open = true;
}

void RpcClient::OnMessage(const websocketpp::connection_hdl &hdl, const message_ptr msg) {
	auto &payload = msg->get_payload();
	MemoryStream wss_read_stream((data_ptr_t)payload.data(), payload.size());
	auto received_message = ProtocolMessage::FromMemoryStream(wss_read_stream);
	std::unique_lock<std::mutex> lock(messages_mutex);
	messages.push_front(std::move(received_message));
	messages_wait.notify_one();
}

// boo
void RpcClient::OnFail(websocketpp::connection_hdl hdl) {
	client::connection_ptr con = websocket_client.get_con_from_hdl(hdl);
	// there is more error stuff to expose here if required
	throw IOException("RPC request to %s failed: %s", uri, con->get_ec().message().c_str());
}

unique_ptr<ProtocolMessage> RpcClient::WaitForMessageInternal(MessageType expected_type) {
	unique_ptr<ProtocolMessage> result;
	if (mode == WEB_SOCKET) {
		std::unique_lock<std::mutex> lock(messages_mutex);
		messages_wait.wait(lock, [=] { return !messages.empty(); });
		result = std::move(messages.back());
		messages.pop_back();
	}
	if (mode == UNIX_SOCKET) {
		result = ProtocolMessage::FromSocket(unix_socket_fd, read_stream);
	}
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

void RpcClient::Send(unique_ptr<ProtocolMessage> message_p) {
	D_ASSERT(message_p);
	if (mode == WEB_SOCKET) {
		try {
			// we have to wait till the connection is actually open on connect
			while (!connection_open) {
				usleep(10);
			}

			message_p->ToMemoryStream(write_stream);
			websocket_client.send(websocket_connection, write_stream.GetData(), write_stream.GetPosition(),
			                      websocketpp::frame::opcode::binary);
		} catch (websocketpp::exception const &e) {
			throw IOException(e.what());
		}
	}
	if (mode == UNIX_SOCKET) {
		message_p->ToSocket(unix_socket_fd, write_stream);
	}
}
