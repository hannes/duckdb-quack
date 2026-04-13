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

HttpsRpcClient::HttpsRpcClient(const RpcUri &uri_p) : RpcClient(uri_p) {
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

	int64_t start_time = 0;
	bool should_log_http = context && Logger::Get(*context).ShouldLog(HTTPLogType::NAME, HTTPLogType::LEVEL);
	if (should_log_http) {
		start_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
		                 .time_since_epoch()
		                 .count();
	}

	auto https_result = https_client->Post("/rpc", (const char *)write_stream.GetData(), write_stream.GetPosition(),
	                                       "application/duckdb");

	if (should_log_http) {
		int64_t end_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
		                       .time_since_epoch()
		                       .count();
		int64_t duration_ms = end_time - start_time;
		int status = https_result ? https_result->status : -1;

		child_list_t<Value> request_child_list = {
		    {"type", Value("POST")}, {"url", Value(uri.Http() + "/rpc")},
		    {"start_time", Value()}, {"duration_ms", Value::BIGINT(duration_ms)},
		    {"headers", Value()},
		};
		auto request_value = Value::STRUCT(request_child_list);

		child_list_t<Value> response_child_list = {
		    {"status", Value(to_string(status))},
		    {"reason", https_result ? Value(https_result->reason) : Value()},
		    {"headers", Value()},
		};
		auto response_value = Value::STRUCT(response_child_list);

		child_list_t<Value> child_list = {{"request", request_value}, {"response", response_value}};
		auto msg = Value::STRUCT(child_list).ToString();
		Logger::Get(*context).WriteLog(HTTPLogType::NAME, HTTPLogType::LEVEL, msg);
	}

	if (!https_result) {
		throw IOException("Failed to send message %s ", to_string(https_result.error()));
	}
	MemoryStream non_owning_read_stream((data_ptr_t)https_result->body.data(), https_result->body.size());
	return ProtocolMessage::FromMemoryStream(non_owning_read_stream);
}

unique_ptr<RpcClient> RpcClient::GetClient(const RpcUri &uri) {
	return make_uniq<HttpsRpcClient>(uri);
}
