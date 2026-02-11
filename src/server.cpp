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
	struct sockaddr_un local;

	auto socket_path = "/tmp/duckdb-rpc-socket";

	auto s = socket(AF_UNIX, SOCK_STREAM, 0);
	local.sun_family = AF_UNIX;

	memset(&local, 0, sizeof(struct sockaddr_un));

	strcpy(local.sun_path, socket_path);
	// TODO check errors on this one
	unlink(local.sun_path);
	auto len = SUN_LEN(&local);
	if (bind(s, (struct sockaddr *)&local, len)) {
		throw std::runtime_error("Error on binding socket \n");
	}

	if (listen(s, 42 /* TODO: magic constant */)) {
		throw std::runtime_error("Error on listen call \n");
	}

	printf("Listening on %s\n", local.sun_path);

	while (true) {
		int client_socket_fd = 0;

		unsigned int sock_len = 0;
		if ((client_socket_fd = accept(s, (struct sockaddr *)&local, &sock_len)) == -1) {
			continue;
		}

		// TODO fork off thread

		MemoryStream read_stream;

		// TODO this is duplicated in client!!

		int data_recv = 0;
		do {
			idx_t msg_len;
			data_recv = recv(client_socket_fd, &msg_len, sizeof(idx_t), MSG_WAITALL);
			if (data_recv != sizeof(idx_t)) {
				printf("Error on recv() call 1\n");
				break; // TODO we probably want to close the connection in this case
			}
			read_stream.GrowCapacity(msg_len);

			data_recv = recv(client_socket_fd, (void *)read_stream.GetData(), msg_len, MSG_WAITALL);
			if (data_recv != msg_len) {
				printf("Error on recv() call 2\n");

				break; // TODO we probably want to close the connection in this case
			}
			auto received_message = ProtocolMessage::FromMemoryStream(read_stream);
			rpc_server->HandleMessage(*received_message, [&](unique_ptr<ProtocolMessage> response_message) -> void {
				response_message->ToMemoryStream(rpc_server->write_stream);
				idx_t msg_len = rpc_server->write_stream.GetPosition();
				if (send(client_socket_fd, &msg_len, sizeof(idx_t), 0) != sizeof(idx_t)) {
					printf("Error 1 on send() call %s \n", strerror(errno));
				}
				if (send(client_socket_fd, rpc_server->write_stream.GetData(), msg_len, 0) != msg_len) {
					printf("Error 2 on send() call %s \n", strerror(errno));
				}
			});

		} while (data_recv > 0); // TODO this is blocking right?
		                         // close(client_socket_fd);
	}
}

void RpcServer::Listen(uint32_t port) {
	s.listen(port);

	// TODO make this cancellable
	listen_thread = std::thread([=]() {
		ListenThread(this);
		return 1;
	});

	// TODO make this cancellable
	unix_socket_thread = std::thread([=]() {
		ListenUnixSocketThread(this);
		return 1;
	});
}

// main switcheroo happens here
void RpcServer::HandleMessage(ProtocolMessage &received_message,
                              std::function<void(unique_ptr<ProtocolMessage>)> send_fun) {
	switch (received_message.Type()) {
	case MessageType::BIND_REQUEST: {
		auto bind_request_message = received_message.Cast<BindRequestMessage>();
		// TODO: does this have to happen in a background thread? Is there going to be an async api for this?
		auto prepare_result = internal_connection.Prepare(bind_request_message.Query());
		unique_ptr<ProtocolMessage> response_message;
		if (prepare_result->HasError()) {
			response_message = make_uniq<ErrorMessage>(prepare_result->GetError());
		} else {
			response_message = make_uniq<BindResponseMessage>(prepare_result->GetTypes(), prepare_result->GetNames());
		}
		send_fun(std::move(response_message));
		return;
	}
		// TODO this currently does not do a whole lot....
	case MessageType::EXECUTE_REQUEST: {
		auto execute_request_message = received_message.Cast<ExecuteRequestMessage>();

		// TODO we need to cache this connection in the ws connection somehow
		// TODO: does this have to happen in a background thread? Is there going to be an async api for this?
		query_result = internal_connection.PendingQuery(execute_request_message.Query(), true)->Execute();
		unique_ptr<ProtocolMessage> response_message;

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
			unique_ptr<ProtocolMessage> response_message;

			if (query_result->HasError()) {
				send_fun(std::move(make_uniq<ErrorMessage>(query_result->GetError())));
				return;
			} else {
				if (!result_chunk || result_chunk->size() == 0) {
					send_fun(std::move(make_uniq<FetchDoneMessage>()));
					return;
				} else {
					response_message = make_uniq<FetchResponseMessage>(std::move(result_chunk));
				}
			}
			// FIXME send column data collection
			send_fun(std::move(response_message));
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
