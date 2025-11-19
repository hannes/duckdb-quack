#define DUCKDB_EXTENSION_MAIN

#include "rpc_extension.hpp"
#include "duckdb.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#define ASIO_STANDALONE // no boost!

#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "websocketpp/config/asio.hpp"
#include "websocketpp/server.hpp"

#include "websocketpp/config/asio_client.hpp"
#include "websocketpp/client.hpp"

#include <iostream>

typedef websocketpp::server<websocketpp::config::asio_tls> server;
typedef websocketpp::client<websocketpp::config::asio_tls_client> client;
typedef websocketpp::config::asio_client::message_type::ptr message_ptr;
typedef websocketpp::lib::shared_ptr<websocketpp::lib::asio::ssl::context> context_ptr;

using websocketpp::lib::bind;
using websocketpp::lib::placeholders::_1;
using websocketpp::lib::placeholders::_2;

namespace duckdb {

// TODO split this up in separate messages
// TODO generate this like the rest

enum class MessageType : uint8_t { INVALID = 0, BIND = 1, BIND_RESULT = 2, EXECUTE = 3, EXECUTE_RESULT = 4, ERROR = 5 };

struct ProtocolMessage {
	void Serialize(Serializer &serializer) {

		serializer.WriteProperty<uint8_t>(1, "type", static_cast<uint8_t>(type));
		serializer.WriteProperty<string>(2, "query", query);
		serializer.WriteProperty<string>(3, "error", error);
		serializer.WriteProperty<vector<LogicalType>>(4, "types", types);
		serializer.WriteProperty<vector<string>>(5, "names", names);
		if (type == MessageType::EXECUTE_RESULT) {
			serializer.WriteObject(6, "data", [&](Serializer &serializer2) { data->Serialize(serializer2); });
		}
	}

	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer) {
		auto result = make_uniq<ProtocolMessage>();
		result->type = static_cast<MessageType>(deserializer.ReadProperty<uint8_t>(1, "type"));
		result->query = deserializer.ReadProperty<string>(2, "query");
		result->error = deserializer.ReadProperty<string>(3, "error");
		result->types = deserializer.ReadProperty<vector<LogicalType>>(4, "types");
		result->names = deserializer.ReadProperty<vector<string>>(5, "names");
		if (result->type == MessageType::EXECUTE_RESULT) {
			result->data = make_uniq<DataChunk>();
			deserializer.ReadObject(6, "data",
			                        [&](Deserializer &deserializer2) { result->data->Deserialize(deserializer2); });
		}
		return result;
	}
	MessageType type;
	string query;
	string error;
	vector<string> names;
	vector<LogicalType> types;
	unique_ptr<DataChunk> data;
};

std::string get_password() {
	throw std::runtime_error("get_password called without a valid password");
}

// See https://wiki.mozilla.org/Security/Server_Side_TLS for more details about
// the TLS modes. The code below demonstrates how to implement both the modern
enum tls_mode { MOZILLA_INTERMEDIATE = 1, MOZILLA_MODERN = 2 };

context_ptr on_tls_init_server(tls_mode mode, websocketpp::connection_hdl hdl) {
	namespace asio = websocketpp::lib::asio;

	// std::cout << "on_tls_init called with hdl: " << hdl.lock().get() << std::endl;
	// std::cout << "using TLS mode: " << (mode == MOZILLA_MODERN ? "Mozilla Modern" : "Mozilla Intermediate") <<
	// std::endl;

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

struct RpcServer {
	RpcServer(ClientContext &context_p, server &s_p) : context(context_p), s(s_p) {
		s.set_message_handler(bind(&RpcServer::on_message, this, ::_1, ::_2));
	}

	// main switcheroo happens here
	void on_message(websocketpp::connection_hdl hdl, message_ptr msg) {
		MemoryStream read_stream(data_ptr_cast((void *)msg->get_payload().data()),
		                         static_cast<idx_t>(msg->get_payload().size()));
		BinaryDeserializer deserializer(read_stream);
		auto received_message = ProtocolMessage::Deserialize(deserializer);

		//		printf("message type %lld\n", (uint8_t)received_message->type);

		switch (received_message->type) {
		case MessageType::BIND: {
			D_ASSERT(received_message->query.size() > 0);
			printf("BIND %s\n", received_message->query.c_str());

			ProtocolMessage response_message;
			response_message.type = MessageType::BIND_RESULT;

			auto internal_connection = Connection(*context.db);
			// TODO: does this have to happen in a background thread? Is there going to be an async api for this?
			auto prepare_result = internal_connection.Prepare(received_message->query);

			response_message.types = prepare_result->GetTypes();
			response_message.names = prepare_result->GetNames();

			MemoryStream write_stream; // TODO pass allocator here
			BinarySerializer serializer(write_stream);
			serializer.Begin();
			response_message.Serialize(serializer);
			serializer.End();

			try {
				s.send(hdl, write_stream.GetData(), write_stream.GetPosition(), websocketpp::frame::opcode::binary);
			} catch (websocketpp::exception const &e) {
				// TODO we should not fail here but log something
				std::cout << "bind reply failed because: "
				          << "(" << e.what() << ")" << std::endl;
			}
			break;
		}
		case MessageType::EXECUTE: {
			D_ASSERT(received_message->query.size() > 0);
			printf("EXECUTE %s\n", received_message->query.c_str());

			ProtocolMessage response_message;
			response_message.type = MessageType::EXECUTE_RESULT;

			// TODO we need to cache this connection in the ws connection somehow
			auto internal_connection = Connection(*context.db);
			// TODO: does this have to happen in a background thread? Is there going to be an async api for this?
			auto execute_result = internal_connection.Query(received_message->query);

			response_message.data = execute_result->Fetch();

			// TODO many
			MemoryStream write_stream; // TODO pass allocator here
			BinarySerializer serializer(write_stream);
			serializer.Begin();
			response_message.Serialize(serializer);
			serializer.End();

			try {
				s.send(hdl, write_stream.GetData(), write_stream.GetPosition(), websocketpp::frame::opcode::binary);
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
	ClientContext &context;
	server &s;
};

inline void RpcScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	server s;
	s.set_access_channels(websocketpp::log::alevel::none);
	s.init_asio();
	RpcServer rcp_server(state.GetContext(), s);
	s.set_tls_init_handler(bind(&on_tls_init_server, MOZILLA_INTERMEDIATE, ::_1));
	int port = 4242;
	s.listen(port);
	s.start_accept();
	printf("Listening on port %d\n", port);
	s.run();
}

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

struct RpcClient {
	RpcClient(string &uri_p) : uri(uri_p) {

		c.set_access_channels(websocketpp::log::alevel::none);
		c.set_error_channels(websocketpp::log::alevel::none);
		// c.clear_access_channels(websocketpp::log::alevel::frame_payload);

		// Initialize ASIO
		c.init_asio();
		c.set_tls_init_handler(bind(&on_tls_init_client, "localhost", ::_1));
		c.set_user_agent("DuckDB");
		c.set_message_handler(bind(&RpcClient::OnMessage, this, ::_1, ::_2));
		c.set_fail_handler(bind(&RpcClient::OnFail, this, ::_1));

		c.set_open_handler(bind(&RpcClient::OnOpen, this, ::_1));

		conn_thread = std::thread([=]() {
			ConnectionThread(this);
			return 1;
		});
	}

	static void ConnectionThread(void *rpc_client_p) {
		auto rpc_client = (RpcClient *)rpc_client_p;
		D_ASSERT(rpc_client);

		websocketpp::lib::error_code ec;
		rpc_client->con = rpc_client->c.get_connection(rpc_client->uri, ec);
		if (ec) {
			throw InternalException(ec.message());
		}
		rpc_client->c.connect(rpc_client->con);
		rpc_client->c.run();
	}

	~RpcClient() {
		if (con) {
			con->close(websocketpp::close::status::normal, "");
		}
		conn_thread.join();
	}

	void OnOpen(websocketpp::connection_hdl hdl) {
		SendInternal(hdl);
	}

	void OnMessage(websocketpp::connection_hdl hdl, message_ptr msg) {
		MemoryStream read_stream(data_ptr_cast((void *)msg->get_payload().data()), msg->get_payload().size());
		BinaryDeserializer deserializer(read_stream);
		auto received_message = ProtocolMessage::Deserialize(deserializer);
		// printf("REC %d\n", received_message->type);
		std::unique_lock<std::mutex> lock(messages_mutex);
		messages.push_front(std::move(received_message));
		messages_wait.notify_one();
		SendInternal(hdl);
	}

	// boo
	void OnFail(websocketpp::connection_hdl hdl) {
		client::connection_ptr con = c.get_con_from_hdl(hdl);
		// TODO there is more error stuff to expose here if required
		throw InvalidInputException("RPC request failed: %s", con->get_ec().message().c_str());
	}

	unique_ptr<ProtocolMessage> WaitForMessage() {
		std::unique_lock<std::mutex> lock(messages_mutex);
		messages_wait.wait(lock, [=] { return !messages.empty(); });
		auto result(std::move(messages.back()));
		messages.pop_back();
		return result;
	}

	void SendInternal(websocketpp::connection_hdl hdl) {
		if (!message) {
			return;
		}
		auto write_stream = make_uniq<MemoryStream>(); // TODO pass allocator here
		BinarySerializer serializer(*write_stream);
		serializer.Begin();
		message->Serialize(serializer);
		serializer.End();

		try {
			c.send(hdl, write_stream->GetData(), write_stream->GetPosition(), websocketpp::frame::opcode::binary);
		} catch (websocketpp::exception const &e) {
			throw InvalidInputException(e.what());
		}
		message.reset();
	}

	void Schedule(unique_ptr<ProtocolMessage> message_p) {
		message = std::move(message_p);
	}

	// TODO too much overlap with SendInternal
	void Send(unique_ptr<ProtocolMessage> message_p) {
		if (!message_p) {
			return;
		}
		auto write_stream = make_uniq<MemoryStream>(); // TODO pass allocator here
		BinarySerializer serializer(*write_stream);
		serializer.Begin();
		message_p->Serialize(serializer);
		serializer.End();

		try {
			c.send(con, write_stream->GetData(), write_stream->GetPosition(), websocketpp::frame::opcode::binary);
		} catch (websocketpp::exception const &e) {
			throw InvalidInputException(e.what());
		}
	}

	std::thread conn_thread;
	unique_ptr<ProtocolMessage> message;
	deque<unique_ptr<ProtocolMessage>> messages;
	std::mutex messages_mutex;
	std::condition_variable messages_wait;
	string uri;
	client c;
	client::connection_ptr con;
};

struct RpcTableBindData : FunctionData {
	explicit RpcTableBindData() {
	}

	unique_ptr<RpcClient> client;
	string query;

	bool Equals(const FunctionData &other_p) const override {
		throw NotImplementedException("Equals not implemented");
	}

	unique_ptr<FunctionData> Copy() const override {
		throw NotImplementedException("Copy not implemented");
	}
};

static unique_ptr<FunctionData> RpcTableBindFun(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {

	// Set logging to be pretty verbose (everything except message payloads)
	auto uri = input.inputs[0].GetValue<string>();
	auto query = input.inputs[1].GetValue<string>();
	auto client = make_uniq<RpcClient>(uri);

	auto bind_message = make_uniq<ProtocolMessage>();
	bind_message->type = MessageType::BIND;
	bind_message->query = query;

	client->Schedule(std::move(bind_message));

	auto bind_response = client->WaitForMessage();
	if (bind_response->type != MessageType::BIND_RESULT) {
		throw InvalidInputException("Expected bind result message");
	}
	return_types = bind_response->types;
	names = bind_response->names;

	auto res = make_uniq<RpcTableBindData>();
	res->client = std::move(client);
	res->query = query;

	return res;
}

static void RpcTableFun(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->Cast<RpcTableBindData>();
	auto &client = *bind_data.client;

	auto execute_message = make_uniq<ProtocolMessage>();
	execute_message->type = MessageType::EXECUTE;
	execute_message->query = bind_data.query;

	client.Send(std::move(execute_message));
	auto execute_response = client.WaitForMessage();

	output.Reference(*execute_response->data);
	output.SetCardinality(execute_response->data->size());
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto rpc_scalar_function = ScalarFunction("start_rpc_server", {}, LogicalType::VARCHAR, RpcScalarFun);
	loader.RegisterFunction(rpc_scalar_function);

	auto rpc_table_function =
	    TableFunction("call_rpc_server", {LogicalType::VARCHAR, LogicalType::VARCHAR}, RpcTableFun, RpcTableBindFun);
	loader.RegisterFunction(rpc_table_function);
}

void RpcExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string RpcExtension::Name() {
	return "rpc";
}

std::string RpcExtension::Version() const {
#ifdef EXT_VERSION_RPC
	return EXT_VERSION_RPC;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(rpc, loader) {
	LoadInternal(loader);
}
}
