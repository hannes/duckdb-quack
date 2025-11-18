#define DUCKDB_EXTENSION_MAIN

#include "rpc_extension.hpp"
#include "duckdb.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#define ASIO_STANDALONE // no boost!



#include <websocketpp/config/asio.hpp>

#include <websocketpp/server.hpp>

#include <iostream>

typedef websocketpp::server<websocketpp::config::asio_tls> server;

using websocketpp::lib::placeholders::_1;
using websocketpp::lib::placeholders::_2;
using websocketpp::lib::bind;

// pull out the type of messages sent by our config
typedef websocketpp::config::asio::message_type::ptr message_ptr;
typedef websocketpp::lib::shared_ptr<websocketpp::lib::asio::ssl::context> context_ptr;

void on_message(server* s, websocketpp::connection_hdl hdl, message_ptr msg) {
    std::cout << "on_message called with hdl: " << hdl.lock().get()
              << " and message: " << msg->get_payload()
              << std::endl;

    try {
        s->send(hdl, msg->get_payload(), msg->get_opcode());
    } catch (websocketpp::exception const & e) {
        std::cout << "Echo failed because: "
                  << "(" << e.what() << ")" << std::endl;
    }
}

void on_http(server* s, websocketpp::connection_hdl hdl) {
    server::connection_ptr con = s->get_con_from_hdl(hdl);
    con->set_body("This is a WebSocket server. Please use an appropriate client.");
    con->set_status(websocketpp::http::status_code::ok);
}

std::string get_password() {
	throw std::runtime_error("get_password called without a valid password");
}

// See https://wiki.mozilla.org/Security/Server_Side_TLS for more details about
// the TLS modes. The code below demonstrates how to implement both the modern
enum tls_mode {
    MOZILLA_INTERMEDIATE = 1,
    MOZILLA_MODERN = 2
};

context_ptr on_tls_init(tls_mode mode, websocketpp::connection_hdl hdl) {
    namespace asio = websocketpp::lib::asio;

    std::cout << "on_tls_init called with hdl: " << hdl.lock().get() << std::endl;
    std::cout << "using TLS mode: " << (mode == MOZILLA_MODERN ? "Mozilla Modern" : "Mozilla Intermediate") << std::endl;

    context_ptr ctx = websocketpp::lib::make_shared<asio::ssl::context>(asio::ssl::context::sslv23);

    try {
        if (mode == MOZILLA_MODERN) {
            // Modern disables TLSv1
            ctx->set_options(asio::ssl::context::default_workarounds |
                             asio::ssl::context::no_sslv2 |
                             asio::ssl::context::no_sslv3 |
                             asio::ssl::context::no_tlsv1 |
                             asio::ssl::context::single_dh_use);
        } else {
            ctx->set_options(asio::ssl::context::default_workarounds |
                             asio::ssl::context::no_sslv2 |
                             asio::ssl::context::no_sslv3 |
                             asio::ssl::context::single_dh_use);
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
            ciphers = "ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-AES256-GCM-SHA384:DHE-RSA-AES128-GCM-SHA256:DHE-DSS-AES128-GCM-SHA256:kEDH+AESGCM:ECDHE-RSA-AES128-SHA256:ECDHE-ECDSA-AES128-SHA256:ECDHE-RSA-AES128-SHA:ECDHE-ECDSA-AES128-SHA:ECDHE-RSA-AES256-SHA384:ECDHE-ECDSA-AES256-SHA384:ECDHE-RSA-AES256-SHA:ECDHE-ECDSA-AES256-SHA:DHE-RSA-AES128-SHA256:DHE-RSA-AES128-SHA:DHE-DSS-AES128-SHA256:DHE-RSA-AES256-SHA256:DHE-DSS-AES256-SHA:DHE-RSA-AES256-SHA:!aNULL:!eNULL:!EXPORT:!DES:!RC4:!3DES:!MD5:!PSK";
        } else {
            ciphers = "ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-AES256-GCM-SHA384:DHE-RSA-AES128-GCM-SHA256:DHE-DSS-AES128-GCM-SHA256:kEDH+AESGCM:ECDHE-RSA-AES128-SHA256:ECDHE-ECDSA-AES128-SHA256:ECDHE-RSA-AES128-SHA:ECDHE-ECDSA-AES128-SHA:ECDHE-RSA-AES256-SHA384:ECDHE-ECDSA-AES256-SHA384:ECDHE-RSA-AES256-SHA:ECDHE-ECDSA-AES256-SHA:DHE-RSA-AES128-SHA256:DHE-RSA-AES128-SHA:DHE-DSS-AES128-SHA256:DHE-RSA-AES256-SHA256:DHE-DSS-AES256-SHA:DHE-RSA-AES256-SHA:AES128-GCM-SHA256:AES256-GCM-SHA384:AES128-SHA256:AES256-SHA256:AES128-SHA:AES256-SHA:AES:CAMELLIA:DES-CBC3-SHA:!aNULL:!eNULL:!EXPORT:!DES:!RC4:!MD5:!PSK:!aECDH:!EDH-DSS-DES-CBC3-SHA:!EDH-RSA-DES-CBC3-SHA:!KRB5-DES-CBC3-SHA";
        }

        if (SSL_CTX_set_cipher_list(ctx->native_handle() , ciphers.c_str()) != 1) {
            std::cout << "Error setting cipher list" << std::endl;
        }
    } catch (std::exception& e) {
        std::cout << "Exception: " << e.what() << std::endl;
    }
    return ctx;
}



namespace duckdb {

inline void RpcScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	// Create a server endpoint
	server echo_server;

	// Initialize ASIO
	echo_server.init_asio();

	// Register our message handler
	echo_server.set_message_handler(bind(&on_message,&echo_server,::_1,::_2));
	echo_server.set_http_handler(bind(&on_http,&echo_server,::_1));
	echo_server.set_tls_init_handler(bind(&on_tls_init,MOZILLA_INTERMEDIATE,::_1));

	// Listen on port 9002
	echo_server.listen(4242);

	// Start the server accept loop
	echo_server.start_accept();

	// Start the ASIO io_service run loop
	echo_server.run();

}



static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto rpc_scalar_function = ScalarFunction("start_rpc_server", {}, LogicalType::VARCHAR, RpcScalarFun);
	loader.RegisterFunction(rpc_scalar_function);

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
	duckdb::LoadInternal(loader);
}
}
