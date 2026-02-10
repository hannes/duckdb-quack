#include "client.hpp"

using namespace duckdb;

using websocketpp::lib::bind;
using websocketpp::lib::placeholders::_1;
using websocketpp::lib::placeholders::_2;

static context_ptr on_tls_init_client(const char *hostname, websocketpp::connection_hdl) {
	context_ptr ctx = websocketpp::lib::make_shared<asio::ssl::context>(asio::ssl::context::sslv23);
	try {
		// TODO is this required??
		ctx->set_options(asio::ssl::context::default_workarounds | asio::ssl::context::no_sslv2 |
		                 asio::ssl::context::no_sslv3 | asio::ssl::context::single_dh_use);

		ctx->set_verify_mode(asio::ssl::verify_none);
	} catch (std::exception &e) {
		throw InvalidInputException(e.what());
	}
	return ctx;
}

static void ConnectionThread(void *rpc_client_p) {
	auto rpc_client = (RpcClient *)rpc_client_p;
	D_ASSERT(rpc_client);

	websocketpp::lib::error_code ec;
	rpc_client->con = rpc_client->c.get_connection(rpc_client->uri, ec);
	if (ec) {
		throw InternalException(ec.message());
	}
	// actually listen and run event loop
	rpc_client->c.connect(rpc_client->con);
	rpc_client->c.run();
}

RpcClient::RpcClient(string &uri_p, Mode mode_p) : uri(uri_p), mode(mode_p) {
	if (mode == WEB_SOCKET) {
		// some ugly setup
		c.set_access_channels(websocketpp::log::alevel::none);
		c.set_error_channels(websocketpp::log::alevel::none);
		c.init_asio();
		c.set_tls_init_handler(bind(&on_tls_init_client, "localhost", ::_1));
		c.set_user_agent("DuckDB");
		c.set_message_handler(bind(&RpcClient::OnMessage, this, ::_1, ::_2));
		c.set_fail_handler(bind(&RpcClient::OnFail, this, ::_1));
		c.set_open_handler(bind(&RpcClient::OnOpen, this, ::_1));

		// launch ze background thread to listen
		conn_thread = std::thread([=]() {
			ConnectionThread(this);
			return 1;
		});
	}
	if (mode == UNIX_SOCKET) {
		struct sockaddr_un remote;
		unix_socket = 0;
		memset(&remote, 0, sizeof(struct sockaddr_un));

		if ((unix_socket = socket(AF_UNIX, SOCK_STREAM, 0)) < 0) {
			throw InternalException("Client: Error on socket() call %s", strerror(errno));
		}
		remote.sun_family = AF_UNIX;
		strcpy(remote.sun_path, uri.c_str());
		auto data_len = SUN_LEN(&remote);

		if (connect(unix_socket, (struct sockaddr *)&remote, data_len)) {
			throw InternalException("Client: Error on connect(%s) call: %s", (char *)remote.sun_path, strerror(errno));
		}
	}
}

RpcClient::~RpcClient() {
	if (mode == WEB_SOCKET) {
		if (con) {
			con->close(websocketpp::close::status::normal, "");
		}
		conn_thread.join();
	}
	if (mode == UNIX_SOCKET) {
		// TODO check for errors etc.
		printf("close()\n");
		close(unix_socket);
	}
}

void RpcClient::OnOpen(websocketpp::connection_hdl hdl) {
	SendInternal(hdl);
}

void RpcClient::OnMessage(websocketpp::connection_hdl hdl, message_ptr msg) {
	auto received_message = ProtocolMessage::FromPayload(msg->get_payload());
	//	printf("REC %d\n", received_message->type);
	std::unique_lock<std::mutex> lock(messages_mutex);
	messages.push_front(std::move(received_message));
	messages_wait.notify_one();
	SendInternal(hdl);
}

// boo
void RpcClient::OnFail(websocketpp::connection_hdl hdl) {
	client::connection_ptr con = c.get_con_from_hdl(hdl);
	// TODO there is more error stuff to expose here if required
	throw InvalidInputException("RPC request failed: %s", con->get_ec().message().c_str());
}

unique_ptr<ProtocolMessage> RpcClient::WaitForMessage() {
	if (mode == WEB_SOCKET) {
		std::unique_lock<std::mutex> lock(messages_mutex);
		messages_wait.wait(lock, [=] { return !messages.empty(); });
		auto result(std::move(messages.back()));
		messages.pop_back();
		return result;
	}
	if (mode == UNIX_SOCKET) {
		std::string message_data;

		int data_recv = 0;
		idx_t msg_len;
		data_recv = recv(unix_socket, &msg_len, sizeof(idx_t), 0);
		if (data_recv != sizeof(idx_t)) {
			throw std::runtime_error("Receive error 1");
		}
		message_data.resize(msg_len);

		data_recv = recv(unix_socket, (void *)message_data.data(), msg_len, 0);
		if (data_recv != msg_len) {
			throw std::runtime_error("Receive error 2");
		}
		return ProtocolMessage::FromPayload(message_data);
	}
	throw InternalException("Invalid mode");
}

void RpcClient::SendInternal(websocketpp::connection_hdl hdl) {
	printf("send\n");

	if (!message) {
		return;
	}
	MemoryStream write_stream;
	message->ToMemoryStream(write_stream);
	try {
		c.send(hdl, write_stream.GetData(), write_stream.GetPosition(), websocketpp::frame::opcode::binary);
	} catch (websocketpp::exception const &e) {
		throw InvalidInputException(e.what());
	}

	message.reset();
}

void RpcClient::Schedule(unique_ptr<ProtocolMessage> message_p) {
	if (mode == WEB_SOCKET) {
		message = std::move(message_p);
	}
	if (mode == UNIX_SOCKET) {
		printf("send\n");

		// TODO loads of overlap
		MemoryStream write_stream;
		message_p->ToMemoryStream(write_stream);
		idx_t msg_len = write_stream.GetPosition();

		if (send(unix_socket, &msg_len, sizeof(idx_t), 0) != sizeof(idx_t)) {
			throw std::runtime_error("Send error 1");
		}
		if (send(unix_socket, write_stream.GetData(), msg_len, 0) != msg_len) {
			throw std::runtime_error("Send error 2");
		}
	}
}

// TODO too much overlap with SendInternal
void RpcClient::Send(unique_ptr<ProtocolMessage> message_p) {
	printf("send\n");
	MemoryStream write_stream;
	message_p->ToMemoryStream(write_stream);
	if (mode == WEB_SOCKET) {
		if (!message_p) {
			return;
		}

		try {
			c.send(con, write_stream.GetData(), write_stream.GetPosition(), websocketpp::frame::opcode::binary);
		} catch (websocketpp::exception const &e) {
			throw InvalidInputException(e.what());
		}
	}
	if (mode == UNIX_SOCKET) {
		idx_t msg_len = write_stream.GetPosition();

		if (send(unix_socket, &msg_len, sizeof(idx_t), 0) != sizeof(idx_t)) {
			throw std::runtime_error("Send error 1");
		}
		if (send(unix_socket, write_stream.GetData(), msg_len, 0) != msg_len) {
			throw std::runtime_error("Send error 2");
		}
	}
}
