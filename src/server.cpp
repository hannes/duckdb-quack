#include "server.hpp"
#include "message.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"

using namespace duckdb;

using websocketpp::lib::placeholders::_1;
using websocketpp::lib::placeholders::_2;

static std::string get_password() {
	throw std::runtime_error("get_password called without a valid password");
}

static context_ptr OnTlsInit(tls_mode mode, websocketpp::connection_hdl hdl) {
	namespace asio = websocketpp::lib::asio;

	context_ptr ctx = websocketpp::lib::make_shared<asio::ssl::context>(asio::ssl::context::sslv23);

	try {
		if (mode == MOZILLA_MODERN) {
			// Modern disables TLSv1
			ctx->set_options(asio::ssl::context::default_workarounds | asio::ssl::context::no_sslv2 |
			                 asio::ssl::context::no_sslv3 | asio::ssl::context::no_tlsv1 |
			                 asio::ssl::context::single_dh_use);
		} else {
			ctx->set_options(asio::ssl::context::default_workarounds | asio::ssl::context::no_sslv2 |
			                 asio::ssl::context::no_sslv3 | asio::ssl::context::single_dh_use);
		}
		ctx->set_password_callback(bind(&get_password));
		ctx->use_certificate_chain_file("server.pem");
		ctx->use_private_key_file("key.pem", asio::ssl::context::pem);

		// Example method of generating this file:
		// `openssl dhparam -out dh.pem 2048`
		// Mozilla Intermediate suggests 1024 as the minimum size to use
		// Mozilla Modern suggests 2048 as the minimum size to use.
		ctx->use_tmp_dh_file("dh.pem");

		std::string ciphers;

		if (mode == MOZILLA_MODERN) {
			ciphers = "ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-"
			          "ECDSA-AES256-GCM-SHA384:DHE-RSA-AES128-GCM-SHA256:DHE-DSS-AES128-GCM-SHA256:kEDH+AESGCM:ECDHE-"
			          "RSA-AES128-SHA256:ECDHE-ECDSA-AES128-SHA256:ECDHE-RSA-AES128-SHA:ECDHE-ECDSA-AES128-SHA:ECDHE-"
			          "RSA-AES256-SHA384:ECDHE-ECDSA-AES256-SHA384:ECDHE-RSA-AES256-SHA:ECDHE-ECDSA-AES256-SHA:DHE-RSA-"
			          "AES128-SHA256:DHE-RSA-AES128-SHA:DHE-DSS-AES128-SHA256:DHE-RSA-AES256-SHA256:DHE-DSS-AES256-SHA:"
			          "DHE-RSA-AES256-SHA:!aNULL:!eNULL:!EXPORT:!DES:!RC4:!3DES:!MD5:!PSK";
		} else {
			ciphers = "ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-"
			          "ECDSA-AES256-GCM-SHA384:DHE-RSA-AES128-GCM-SHA256:DHE-DSS-AES128-GCM-SHA256:kEDH+AESGCM:ECDHE-"
			          "RSA-AES128-SHA256:ECDHE-ECDSA-AES128-SHA256:ECDHE-RSA-AES128-SHA:ECDHE-ECDSA-AES128-SHA:ECDHE-"
			          "RSA-AES256-SHA384:ECDHE-ECDSA-AES256-SHA384:ECDHE-RSA-AES256-SHA:ECDHE-ECDSA-AES256-SHA:DHE-RSA-"
			          "AES128-SHA256:DHE-RSA-AES128-SHA:DHE-DSS-AES128-SHA256:DHE-RSA-AES256-SHA256:DHE-DSS-AES256-SHA:"
			          "DHE-RSA-AES256-SHA:AES128-GCM-SHA256:AES256-GCM-SHA384:AES128-SHA256:AES256-SHA256:AES128-SHA:"
			          "AES256-SHA:AES:CAMELLIA:DES-CBC3-SHA:!aNULL:!eNULL:!EXPORT:!DES:!RC4:!MD5:!PSK:!aECDH:!EDH-DSS-"
			          "DES-CBC3-SHA:!EDH-RSA-DES-CBC3-SHA:!KRB5-DES-CBC3-SHA";
		}

		if (SSL_CTX_set_cipher_list(ctx->native_handle(), ciphers.c_str()) != 1) {
			std::cout << "Error setting cipher list" << std::endl;
		}
	} catch (std::exception &e) {
		std::cout << "Exception: " << e.what() << std::endl;
	}
	return ctx;
}

RpcServer::RpcServer(ClientContext &context_p) : context(context_p), internal_connection(*context.db) {
	s.set_access_channels(websocketpp::log::alevel::none);
	s.set_tls_init_handler(bind(&OnTlsInit, MOZILLA_INTERMEDIATE, ::_1));
	s.set_message_handler(bind(&RpcServer::OnMessage, this, ::_1, ::_2));
	s.init_asio();
}

static void ListenThread(void *rpc_server_p) {
	auto rpc_server = (RpcServer *)rpc_server_p;
	D_ASSERT(rpc_server);

	rpc_server->s.start_accept();
	rpc_server->s.run();
}

static void ListenUnixSocketThread(void *rpc_server_p) {
	auto rpc_server = (RpcServer *)rpc_server_p;
	D_ASSERT(rpc_server);

	auto server_socket_fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (server_socket_fd == -1) {
		throw IOException("Error creating socket");
	}

	sockaddr_un socket_address;
	socket_address.sun_family = AF_UNIX;
	memset(&socket_address, 0, sizeof(sockaddr_un));
	strncpy(socket_address.sun_path, rpc_server->listen_string.c_str(), sizeof(socket_address.sun_path) - 1);

	auto unlink_result = unlink(socket_address.sun_path);
	if (unlink_result && errno != ENOENT) {
		throw IOException("Error cleaning up socket %s: %s", (const char *)socket_address.sun_path, strerror(errno));
	}

	if (bind(server_socket_fd, (sockaddr *)&socket_address, SUN_LEN(&socket_address)) ||
	    listen(server_socket_fd, 42 /* TODO: magic constant */)) {
		throw IOException("Error listening to socket %s: %s", (const char *)socket_address.sun_path, strerror(errno));
	}

	while (true) {
		int client_socket_fd = 0;

		unsigned int sock_len = 0;
		if ((client_socket_fd = accept(server_socket_fd, (sockaddr *)&socket_address, &sock_len)) == -1) {
			continue;
		}

		// TODO fork off thread
		bool open = true;
		do {
			try {
				auto received_message = ProtocolMessage::FromSocket(client_socket_fd);
				rpc_server->HandleMessage(*received_message, [&](unique_ptr<ProtocolMessage> response_message) -> void {
					response_message->ToSocket(client_socket_fd);
				});
			} catch (IOException e) {
				open = false;
			}

		} while (open);
		close(client_socket_fd);
	}
}

void RpcServer::Listen(const string &listen_string_p) {
	if (listen_string_p.size() == 0) {
		throw InvalidInputException("Empty listen string specified");
	}
	listen_string = listen_string_p;
	if (StringUtil::StartsWith(listen_string, "wss:")) {
		// TODO this is overly simplistic but fine for now
		auto listen_port = atoi(StringUtil::Replace(listen_string, "wss://localhost:", "").c_str());
		if (listen_port < 1 || listen_port > 65535) {
			throw InvalidInputException("Invalid port specified for websocket server (%d)", listen_port);
		}
		s.listen(listen_port);

		listen_thread = std::thread([=]() {
			ListenThread(this);
			return 1;
		});
	} else {
		unix_socket_thread = std::thread([=]() {
			ListenUnixSocketThread(this);
			return 1;
		});
	}
}

// main switcheroo happens here
void RpcServer::HandleMessage(ProtocolMessage &received_message,
                              std::function<void(unique_ptr<ProtocolMessage>)> send_fun) {
	switch (received_message.Type()) {
	case MessageType::BIND_REQUEST: {
		auto bind_request_message = received_message.Cast<BindRequestMessage>();
		// TODO: does this have to happen in a background thread? Is there going to be an async api for this?
		auto prepare_result = internal_connection.Prepare(bind_request_message.Query());
		if (prepare_result->HasError()) {
			send_fun(make_uniq<ErrorMessage>(prepare_result->GetError()));
			return;
		}
		send_fun(make_uniq<BindResponseMessage>(prepare_result->GetTypes(), prepare_result->GetNames()));
		return;
	}
	case MessageType::EXECUTE_REQUEST: {
		auto execute_request_message = received_message.Cast<ExecuteRequestMessage>();
		// TODO we need to cache this connection in the ws connection somehow
		// TODO: does this have to happen in a background thread? Is there going to be an async api for this?
		query_result = internal_connection.PendingQuery(execute_request_message.Query(), true)->Execute();
		if (query_result->HasError()) {
			send_fun(make_uniq<ErrorMessage>(query_result->GetError()));
			return;
		}
		// TODO add a query handle here
		send_fun(make_uniq<ExecuteResponseMessage>());
		return;
	}

	case MessageType::FETCH_REQUEST: {
		while (true) { // FIXME this just dumps the results on the client without asking for speed
			auto result_chunk = query_result->Fetch();

			if (query_result->HasError()) {
				send_fun(make_uniq<ErrorMessage>(query_result->GetError()));
				return;
			}
			if (!result_chunk || result_chunk->size() == 0) {
				send_fun(make_uniq<FetchResponseMessage>(nullptr));
				return;
			}
			// FIXME send column data collection instead?
			send_fun(make_uniq<FetchResponseMessage>(std::move(result_chunk)));
		}
		return;
	}
	default: {
		throw NotImplementedException("Unimplemented message type %s", MessageTypeToString(received_message.Type()));
	}
	}
}

void RpcServer::OnMessage(websocketpp::connection_hdl hdl, message_ptr msg) {
	MemoryStream read_stream((data_ptr_t)msg->get_payload().data(), msg.get()->get_payload().size());

	auto received_message = ProtocolMessage::FromMemoryStream(read_stream);

	HandleMessage(*received_message, [&](unique_ptr<ProtocolMessage> response_message) -> void {
		response_message->ToMemoryStream(write_stream);
		try {
			s.send(hdl, write_stream.GetData(), write_stream.GetPosition(), websocketpp::frame::opcode::binary);
		} catch (websocketpp::exception const &e) {
			// TODO we should not fail here but log something
			std::cout << "bind reply failed because: "
			          << "(" << e.what() << ")" << std::endl;
		}
	});
}
