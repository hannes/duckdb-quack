#pragma once

#include "message.hpp"
#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.hpp"

namespace duckdb {

class RpcClient {
public:
	explicit RpcClient(const string &uri_p) : uri(uri_p) {};
	template <class TARGET>
	unique_ptr<TARGET> MakeRequest(unique_ptr<ProtocolMessage> request_message) {
		lock_guard<mutex> guard(request_mutex);
		//	printf("C SEND %s\n", MessageTypeToString(request_message->Type()).c_str());
		Send(std::move(request_message));
		auto response_message = WaitForMessageInternal(TARGET::TYPE).release();
		//	printf("C RECV %s\n", MessageTypeToString(response_message->Type()).c_str());

		return unique_ptr<TARGET>(reinterpret_cast<TARGET *>(response_message));
	}

	static unique_ptr<RpcClient> GetClient(const string &uri);

	virtual ~RpcClient() {};

protected:
	mutex request_mutex;
	MemoryStream read_stream, write_stream;
	const string uri;

private:
	unique_ptr<ProtocolMessage> WaitForMessageInternal(MessageType expected_type);
	virtual void Send(unique_ptr<ProtocolMessage> message_p) {};
	virtual unique_ptr<ProtocolMessage> Receive() {
		return nullptr;
	};
};

class UnixSocketRpcClient : public RpcClient {
public:
	UnixSocketRpcClient(const string &uri_p);
	~UnixSocketRpcClient() override;

private:
	void Send(unique_ptr<ProtocolMessage> message_p) override;
	unique_ptr<ProtocolMessage> Receive() override;

private:
	int unix_socket_fd;
};

class HttpsRpcClient : public RpcClient {
public:
	HttpsRpcClient(const string &uri_p);
	~HttpsRpcClient() override;

private:
	void Send(unique_ptr<ProtocolMessage> message_p) override;
	unique_ptr<ProtocolMessage> Receive() override;

private:
	unique_ptr<duckdb_httplib_openssl::Client> https_client;
	duckdb_httplib_openssl::Result https_result;
};

} // namespace duckdb
