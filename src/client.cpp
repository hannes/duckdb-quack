#include "client.hpp"
#include "rpc_uri.hpp"

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.hpp"

using namespace duckdb;

template <class T>
string GetUriPart(T ele) {
	if (ele.afterLast - ele.first < 1) {
		throw InvalidInputException("Invalid URI");
	}
	return string(ele.first, ele.afterLast - ele.first);
}

HttpsRpcClient::HttpsRpcClient(const string &uri_p) : RpcClient(uri_p) {
	https_client = make_uniq<duckdb_httplib_openssl::Client>(uri.Http());
	if (uri.Ssl()) {
		https_client->enable_server_certificate_verification(false);
		https_client->enable_server_hostname_verification(false);
	}
	https_client->set_keep_alive(true);
	https_client->set_follow_location(true);
};

HttpsRpcClient::~HttpsRpcClient() {
	https_client.reset();
}

unique_ptr<ProtocolMessage> HttpsRpcClient::RequestInternal(unique_ptr<ProtocolMessage> request_message) {
	D_ASSERT(request_message);
	request_message->ToMemoryStream(write_stream);
	auto https_result = https_client->Post("/rpc", (const char *)write_stream.GetData(), write_stream.GetPosition(),
	                                       "application/duckdb");
	if (!https_result) {
		throw IOException("Failed to send message %s ", to_string(https_result.error()));
	}
	MemoryStream non_owning_read_stream((data_ptr_t)https_result->body.data(), https_result->body.size());
	return ProtocolMessage::FromMemoryStream(non_owning_read_stream);
}

unique_ptr<RpcClient> RpcClient::GetClient(const string &uri) {
	return make_uniq<HttpsRpcClient>(uri);
}
