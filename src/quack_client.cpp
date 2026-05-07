#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"

#include "quack_client.hpp"
#include "quack_uri.hpp"

using namespace duckdb;

template <class T>
string GetUriPart(T ele) {
	if (ele.afterLast - ele.first < 1) {
		throw InvalidInputException("Invalid URI");
	}
	return string(ele.first, ele.afterLast - ele.first);
}

HttpsQuackClient::HttpsQuackClient(DatabaseInstance &db, const QuackUri &uri_p) : QuackClient(db, uri_p) {};

HttpsQuackClient::~HttpsQuackClient() {
}

unique_ptr<QuackMessage> HttpsQuackClient::RequestInternal(optional_ptr<ClientContext> context,
                                                           unique_ptr<QuackMessage> request_message) {
	D_ASSERT(request_message);

	lock_guard<mutex> guard(request_mutex);

	auto &http_util = HTTPUtil::Get(db);
	auto request_url = uri.Http() + "/quack";
	if (!http_params) {
		if (context) {
			http_params = http_util.InitializeParameters(*context, request_url);
		} else {
			http_params = http_util.InitializeParameters(db, request_url);
		}
	}

	HTTPHeaders headers;

	request_message->ToMemoryStream(write_stream);
	PostRequestInfo post_request(request_url, headers, *http_params, write_stream.GetData(),
	                             write_stream.GetPosition());
	unique_ptr<HTTPResponse> response;

	// Time the request
	int64_t start_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
	                         .time_since_epoch()
	                         .count();

	try {
		response = http_util.Request(post_request);
	} catch (std::exception &e) {
		throw IOException("Failed to send message: %s", e.what());
	}
	if (!response || !response->Success()) {
		string error = response ? response->GetError() : "no response";
		throw IOException("Failed to send message: %s", error);
	}

	MemoryStream non_owning_read_stream((data_ptr_t)post_request.buffer_out.data(), post_request.buffer_out.size());
	auto response_message = QuackMessage::FromMemoryStream(non_owning_read_stream);

	// logging stuff, own scope
	{
		int64_t end_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
		                       .time_since_epoch()
		                       .count();

		auto request_type = request_message->Type();
		string connection_id;
		string query;
		optional_idx client_query_id;
		switch (request_type) {
		case MessageType::PREPARE_REQUEST: {
			auto &msg = request_message->Cast<PrepareRequestMessage>();
			connection_id = msg.ConnectionId();
			query = msg.Query();
			break;
		}
		case MessageType::FETCH_REQUEST:
			connection_id = request_message->Cast<FetchRequestMessage>().ConnectionId();
			break;
		case MessageType::APPEND_REQUEST:
			connection_id = request_message->Cast<AppendRequestMessage>().ConnectionId();
			break;
		default:
			break;
		}

		// Inject client_query_id from context into the message before sending.
		// Guard against reading the active query during transaction start itself
		// (e.g. BEGIN TRANSACTION via QuackCatalog::ExecuteCommand), where the
		// transaction isn't yet installed on the TransactionContext.
		if (context && context->transaction.HasActiveTransaction()) {
			auto raw_query_id = context->transaction.GetActiveQuery();
			if (raw_query_id != DConstants::INVALID_INDEX) {
				client_query_id = raw_query_id;
				request_message->SetClientQueryId(client_query_id);
			}
		}

		// Log RPC message
		auto &logger = context ? Logger::Get(*context) : Logger::Get(db);
		if (logger.ShouldLog(QuackLogType::NAME, QuackLogType::LEVEL)) {
			string error;
			if (response_message->Type() == MessageType::ERROR_RESPONSE) {
				error = response_message->Cast<ErrorResponse>().ErrorMessage();
			}
			auto msg =
			    QuackLogType::ConstructLogMessage(request_type, connection_id, client_query_id, query, uri.Http(),
			                                      end_time - start_time, response_message->Type(), error);
			logger.WriteLog(QuackLogType::NAME, QuackLogType::LEVEL, msg);
		}
	}

	return response_message;
}

unique_ptr<QuackClient> QuackClient::GetClient(DatabaseInstance &db, const QuackUri &uri) {
	ExtensionHelper::AutoLoadExtension(db, "httpfs");
	if (!db.ExtensionIsLoaded("httpfs")) {
		throw MissingExtensionException("The rpc extension requires the httpfs extension to be loaded!");
	}

	return make_uniq<HttpsQuackClient>(db, uri);
}

unique_ptr<QuackClient> QuackClient::GetClient(ClientContext &context, const QuackUri &uri) {
	return GetClient(*context.db, uri);
}
