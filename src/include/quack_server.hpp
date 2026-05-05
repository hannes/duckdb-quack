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
	explicit QuackServer(ClientContext &context_p);
	// TODO should listen be part of the constructor?
	virtual void Listen(const QuackUri &uri) {};

	//! Synchronously stop accepting connections and join the listener threads.
	virtual void Close() {};

	optional_ptr<QuackConnection> GetConnection(const string &connection_id);
	string CreateNewConnection(const string &session_id);
	// TODO need something to destroy connections

	string GenerateSessionId();

	virtual ~QuackServer();

protected:
	unique_ptr<QuackMessage> HandleMessage(QuackMessage &received_message);
	unique_ptr<QuackMessage> HandleMessageInternal(QuackMessage &received_message);

protected:
	std::vector<std::thread> listen_threads;

	shared_ptr<DatabaseInstance> db;
	mutex active_connections_mutex;
	unordered_map<string, unique_ptr<QuackConnection>> active_connections;

	mutex session_id_rng_mutex;
	shared_ptr<EncryptionState> session_id_rng;
};

class HttpQuackServer : public QuackServer {
public:
	HttpQuackServer(ClientContext &context_p) : QuackServer(context_p) {
	}
	void Listen(const QuackUri &uri) override;
	void Close() override;

	~HttpQuackServer() override;

private:
	static void ListenThread(HttpQuackServer *rpc_server, const string &listen_host, int listen_port);

	unique_ptr<duckdb_httplib::Server> server;
};
;

} // namespace duckdb
