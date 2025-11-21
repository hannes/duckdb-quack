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

RpcClient::RpcClient(string &uri_p) : uri(uri_p) {
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

RpcClient::~RpcClient() {
	if (con) {
		con->close(websocketpp::close::status::normal, "");
	}
	conn_thread.join();
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
	std::unique_lock<std::mutex> lock(messages_mutex);
	messages_wait.wait(lock, [=] { return !messages.empty(); });
	auto result(std::move(messages.back()));
	messages.pop_back();
	return result;
}

void RpcClient::SendInternal(websocketpp::connection_hdl hdl) {
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
	message = std::move(message_p);
}

// TODO too much overlap with SendInternal
void RpcClient::Send(unique_ptr<ProtocolMessage> message_p) {
	if (!message_p) {
		return;
	}
	MemoryStream write_stream;
	message_p->ToMemoryStream(write_stream);
	try {
		c.send(con, write_stream.GetData(), write_stream.GetPosition(), websocketpp::frame::opcode::binary);
	} catch (websocketpp::exception const &e) {
		throw InvalidInputException(e.what());
	}
}
