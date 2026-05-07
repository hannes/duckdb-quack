#pragma once

#include <thread>

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/shared_ptr.hpp"

#include "quack_uri.hpp"

#include "httplib.hpp" // TODO forward declare

namespace duckdb {

class ClientContext;
class QuackMessage;
class Connection;
class MemoryStream;
class QueryResult;
class DatabaseInstance;
class PreparedStatement;
class EncryptionState;

struct QuackConnection {
	mutex lock;
	unique_ptr<Connection> duckdb_connection;
	//	unordered_map<string, std::pair<unique_ptr<PreparedStatement>, unique_ptr<QueryResult>>> duckdb_statements;
	unique_ptr<QueryResult> duckdb_query_result;
	//! Monotonic counter assigned per FETCH batch — enables order-preserving parallel scans on
	idx_t next_batch_index = 0;
};

class QuackServer {
public:
	explicit QuackServer(ClientContext &context_p, const QuackUri &uri_p, const string &token_p);

	//! Synchronously stop accepting connections and join the listener threads.
	virtual void Close() {};

	optional_ptr<QuackConnection> GetConnection(const string &connection_id);
	string CreateNewConnection(const string &session_id);
	// TODO need something to destroy connections

	string GenerateSessionId();

	//! Generate a fresh CSPRNG-backed 128-bit token, hex-encoded (32 chars).
	static string GenerateRandomToken(DatabaseInstance &db);

	//! Throw InvalidInputException if `token` doesn't meet requirements(currently, length >= 4)
	static void ValidateToken(const string &token);

	const string &Token() {
		return token;
	}

	virtual ~QuackServer();

protected:
	unique_ptr<QuackMessage> HandleMessage(MemoryStream &read_stream);
	unique_ptr<QuackMessage> HandleMessageInternal(QuackMessage &received_message,
	                                               optional_ptr<QuackConnection> connection);

protected:
	std::vector<std::thread> listen_threads;

	shared_ptr<DatabaseInstance> db;
	mutex active_connections_mutex;
	unordered_map<string, unique_ptr<QuackConnection>> active_connections;

	mutex session_id_rng_mutex;
	shared_ptr<EncryptionState> session_id_rng;

private:
	QuackUri uri;
	string token;
};

class HttpQuackServer : public QuackServer {
public:
	HttpQuackServer(ClientContext &context_p, const QuackUri &uri_p, const string &token_p);

	void Close() override;

	~HttpQuackServer() override;

private:
	static void ListenThread(HttpQuackServer *server, const string &listen_host, int listen_port);

	unique_ptr<QuackMessage> ReadMessage(MemoryStream &read_stream);

	unique_ptr<duckdb_httplib::Server> server;
};
;

} // namespace duckdb
