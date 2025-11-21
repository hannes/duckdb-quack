#include "server.hpp"
#include "message.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"

using namespace duckdb;

using websocketpp::lib::bind;
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

void RpcServer::Listen(uint32_t port) {
	s.listen(port);

	// TODO make this cancellable
	listen_thread = std::thread([=]() {
		ListenThread(this);
		return 1;
	});
}

// main switcheroo happens here
void RpcServer::OnMessage(websocketpp::connection_hdl hdl, message_ptr msg) {

	auto received_message = ProtocolMessage::FromPayload(msg->get_payload());

	switch (received_message->type) {
	case MessageType::BIND: {
		D_ASSERT(received_message->query.size() > 0);
		// printf("BIND %s\n", received_message->query.c_str());

		ProtocolMessage response_message;
		response_message.type = MessageType::BIND_RESULT;

		// TODO: does this have to happen in a background thread? Is there going to be an async api for this?
		auto prepare_result = internal_connection.Prepare(received_message->query);

		response_message.types = prepare_result->GetTypes();
		response_message.names = prepare_result->GetNames();

		auto write_message = response_message.ToMemoryStream();

		try {
			s.send(hdl, write_message->GetData(), write_message->GetPosition(), websocketpp::frame::opcode::binary);
		} catch (websocketpp::exception const &e) {
			// TODO we should not fail here but log something
			std::cout << "bind reply failed because: "
			          << "(" << e.what() << ")" << std::endl;
		}
		break;
	}
		// TODO this currently does not do a whole lot....
	case MessageType::EXECUTE: {
		D_ASSERT(received_message->query.size() > 0);
		// printf("EXECUTE %s\n", received_message->query.c_str());

		ProtocolMessage response_message;
		response_message.type = MessageType::EXECUTE_RESULT;

		// TODO we need to cache this connection in the ws connection somehow
		// TODO: does this have to happen in a background thread? Is there going to be an async api for this?
		query_result = internal_connection.Query(received_message->query);

		if (query_result->HasError()) {
			response_message.type = MessageType::ERROR;
			response_message.error = query_result->GetError();
		}

		//
		// response_message.data = execute_result->Fetch();
		auto write_message = response_message.ToMemoryStream();

		try {
			s.send(hdl, write_message->GetData(), write_message->GetPosition(), websocketpp::frame::opcode::binary);
		} catch (websocketpp::exception const &e) {
			// TODO we should not fail here but log something
			std::cout << "bind reply failed because: "
			          << "(" << e.what() << ")" << std::endl;
		}
		break;
	}

	case MessageType::FETCH: {
		// printf("FETCH\n");

		ProtocolMessage response_message;
		response_message.type = MessageType::FETCH_RESULT;

		// TODO we need to cache this connection in the ws connection somehow
		// TODO: does this have to happen in a background thread? Is there going to be an async api for this?

		response_message.data = query_result->Fetch();
		if (!response_message.data || response_message.data->size() == 0) {
			response_message.type = MessageType::FETCH_DONE;
		}

		if (query_result->HasError()) {
			response_message.type = MessageType::ERROR;
			response_message.error = query_result->GetError();
		}

		auto write_message = response_message.ToMemoryStream();

		try {
			s.send(hdl, write_message->GetData(), write_message->GetPosition(), websocketpp::frame::opcode::binary);
		} catch (websocketpp::exception const &e) {
			// TODO we should not fail here but log something
			std::cout << "bind reply failed because: "
			          << "(" << e.what() << ")" << std::endl;
		}
		break;
	}
	default: {
		printf("eeek!\n");
		// TODO complain, but do not exit
		break;
	}
	}
}
