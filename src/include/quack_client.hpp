#pragma once

#include "duckdb/common/http_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

#include "quack_message.hpp"
#include "quack_log.hpp"
#include "quack_uri.hpp"

namespace duckdb {

class QuackClient {
public:
	explicit QuackClient(ClientContext &context_p, const QuackUri &uri_p) : context(context_p), uri(uri_p) {};

	template <class TARGET>
	unique_ptr<TARGET> Request(unique_ptr<QuackMessage> request_message) {
		auto response_message = RequestInternal(std::move(request_message)).release();
		if (response_message->Type() != TARGET::TYPE) {
			if (response_message->Type() == MessageType::ERROR_RESPONSE) {
				throw IOException("Expected %s message, got error message instead: %s",
				                  MessageTypeToString(TARGET::TYPE),
				                  response_message->Cast<ErrorMessage>().Error().c_str());
			}
			throw IOException("Expected %s message, got %s instead", MessageTypeToString(TARGET::TYPE),
			                  MessageTypeToString(response_message->Type()));
		}
		return unique_ptr<TARGET>(reinterpret_cast<TARGET *>(response_message));
	}

	static unique_ptr<QuackClient> GetClient(ClientContext &context, const QuackUri &uri);

	virtual ~QuackClient() {};

protected:
	mutex request_mutex;
	MemoryStream read_stream, write_stream;
	ClientContext &context;
	QuackUri uri;

private:
	virtual unique_ptr<QuackMessage> RequestInternal(unique_ptr<QuackMessage> request_message) = 0;
};

class HttpsQuackClient : public QuackClient {
public:
	HttpsQuackClient(ClientContext &context, const QuackUri &uri_p);
	~HttpsQuackClient() override;

private:
	unique_ptr<QuackMessage> RequestInternal(unique_ptr<QuackMessage> request_message) override;

private:
	unique_ptr<HTTPParams> http_params;
};

} // namespace duckdb
