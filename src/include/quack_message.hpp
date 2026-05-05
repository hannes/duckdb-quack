#pragma once

#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"

namespace duckdb {

enum class MessageType : uint8_t {
	INVALID = 0,
	CONNECTION_REQUEST = 1,
	CONNECTION_RESPONSE = 2,
	PREPARE_REQUEST = 3,
	PREPARE_RESPONSE = 4,
	FETCH_REQUEST = 7,
	FETCH_RESPONSE = 8,
	CATALOG_REQUEST = 9,
	CATALOG_RESPONSE = 10,
	APPEND_REQUEST = 11,
	APPEND_RESPONSE = 12,
	ERROR = 100
};

string MessageTypeToString(MessageType type);

class QuackMessage {
public:
	void ToMemoryStream(MemoryStream &write_stream) const;
	static unique_ptr<QuackMessage> FromMemoryStream(MemoryStream &read_stream);

	template <class TARGET>
	TARGET &Cast() {
		if (message_type != TARGET::TYPE) {
			throw InternalException("Failed to cast message to type - message type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (message_type != TARGET::TYPE) {
			throw InternalException("Failed to cast message to type - message type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}

	virtual void Serialize(Serializer &serializer) const;
	static unique_ptr<QuackMessage> Deserialize(Deserializer &deserializer);

	const MessageType &Type() const {
		return message_type;
	}

	optional_idx ClientQueryId() const {
		return client_query_id;
	}

	void SetClientQueryId(optional_idx query_id) {
		client_query_id = query_id;
	}

	virtual ~QuackMessage() {
	}

protected:
	explicit QuackMessage(MessageType type) : message_type(type) {
	}
	MessageType message_type = MessageType::INVALID;
	optional_idx client_query_id;
};

class PrepareRequestMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::PREPARE_REQUEST;

	PrepareRequestMessage(const string &connection_id_p, const string &sql_query_p, bool immediately_execute_p = false)
	    : QuackMessage(TYPE), connection_id(connection_id_p), sql_query(sql_query_p),
	      immediately_execute(immediately_execute_p) {
	}
	const std::string &Query() const {
		return sql_query;
	}
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<QuackMessage> Deserialize(Deserializer &deserializer);

	const std::string &ConnectionId() const {
		return connection_id;
	}

	bool ImmediatelyExecute() const {
		return immediately_execute;
	}

private:
	string connection_id; // FIXME abstract this to some superclass
	string sql_query;
	bool immediately_execute;
};

class PrepareResponseMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::PREPARE_RESPONSE;

	PrepareResponseMessage(const vector<LogicalType> &types_p, const vector<string> &names_p,
	                       vector<unique_ptr<DataChunk>> results_p, bool needs_more_fetch_p)
	    : QuackMessage(TYPE), result_types(types_p), result_names(names_p), needs_more_fetch(needs_more_fetch_p),
	      results(std::move(results_p)) {};

	const vector<LogicalType> &Types() const {
		return result_types;
	}

	const vector<string> &Names() const {
		return result_names;
	}

	vector<unique_ptr<DataChunk>> &MutableResults() {
		return results;
	}

	bool NeedsMoreFetch() const {
		return needs_more_fetch;
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<QuackMessage> Deserialize(Deserializer &deserializer);

private:
	vector<LogicalType> result_types;
	vector<string> result_names;
	bool needs_more_fetch;
	vector<unique_ptr<DataChunk>> results;
};

// TODO this is where auth goes
class ConnectionRequestMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::CONNECTION_REQUEST;

	ConnectionRequestMessage(const string &auth_string_p) : QuackMessage(TYPE), auth_string(auth_string_p) {
	}
	const std::string &AuthString() const {
		return auth_string;
	}
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<QuackMessage> Deserialize(Deserializer &deserializer);

private:
	string auth_string;
};

class ConnectionResponseMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::CONNECTION_RESPONSE;

	explicit ConnectionResponseMessage(const string &connection_id_p)
	    : QuackMessage(TYPE), connection_id(connection_id_p) {
	}

	const std::string &ConnectionId() const {
		return connection_id;
	}
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<QuackMessage> Deserialize(Deserializer &deserializer);

private:
	string connection_id;
};

class FetchRequestMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::FETCH_REQUEST;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<QuackMessage> Deserialize(Deserializer &deserializer);
	const std::string &ConnectionId() const {
		return connection_id;
	}
	explicit FetchRequestMessage(const string &connection_id_p) : QuackMessage(TYPE), connection_id(connection_id_p) {};

	// TODO what was this for again?
	// TODO contain the query ref
private:
	string connection_id;
};

class FetchResponseMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::FETCH_RESPONSE;

	FetchResponseMessage() : QuackMessage(TYPE) {};
	explicit FetchResponseMessage(vector<unique_ptr<DataChunk>> results_p)
	    : QuackMessage(TYPE), results(std::move(results_p)) {};
	FetchResponseMessage(vector<unique_ptr<DataChunk>> results_p, optional_idx batch_index_p)
	    : QuackMessage(TYPE), results(std::move(results_p)), batch_index(batch_index_p) {};

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<QuackMessage> Deserialize(Deserializer &deserializer);

	vector<unique_ptr<DataChunk>> &MutableResults() {
		return results;
	}

	optional_idx BatchIndex() const {
		return batch_index;
	}

private:
	vector<unique_ptr<DataChunk>> results;
	optional_idx batch_index;
};

// orrr
static unique_ptr<ParseInfo> ParseInfoCopy(ParseInfo &parse_info) {
	switch (parse_info.info_type) {
	case ParseInfoType::CREATE_INFO: {
		return std::move(parse_info.Cast<CreateInfo>().Copy());
	}
	case ParseInfoType::DROP_INFO: {
		return std::move(parse_info.Cast<DropInfo>().Copy());
	}
	default:
		throw NotImplementedException("Unsupported ParseInfoType");
	}
}

class CatalogRequestMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::CATALOG_REQUEST;

	explicit CatalogRequestMessage(const string &connection_id_p, unique_ptr<ParseInfo> parse_info_p)
	    : QuackMessage(TYPE), connection_id(connection_id_p), parse_info(std::move(parse_info_p)) {};

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<QuackMessage> Deserialize(Deserializer &deserializer);
	unique_ptr<ParseInfo> GetParseInfo() const {
		return ParseInfoCopy(*parse_info);
	}
	const std::string &ConnectionId() const {
		return connection_id;
	}

private:
	string connection_id;
	unique_ptr<ParseInfo> parse_info;
};

class CatalogResponseMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::CATALOG_RESPONSE;

	explicit CatalogResponseMessage(unique_ptr<ParseInfo> parse_info_p)
	    : QuackMessage(TYPE), parse_info(std::move(parse_info_p)) {};

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<QuackMessage> Deserialize(Deserializer &deserializer);
	unique_ptr<ParseInfo> GetParseInfo() const {
		return ParseInfoCopy(*parse_info);
	}

private:
	unique_ptr<ParseInfo> parse_info;
};

class AppendRequestMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::APPEND_REQUEST;

	explicit AppendRequestMessage(const string &connection_id_p, const string &schema_name_p,
	                              const string &table_name_p, unique_ptr<DataChunk> append_chunk_p)
	    : QuackMessage(TYPE), connection_id(connection_id_p), schema_name(schema_name_p), table_name(table_name_p),
	      append_chunk(std::move(append_chunk_p)) {};

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<QuackMessage> Deserialize(Deserializer &deserializer);
	DataChunk &AppendChunk() const {
		return *append_chunk;
	}
	const std::string &ConnectionId() const {
		return connection_id;
	}
	const std::string &SchemaName() const {
		return schema_name;
	}
	const std::string &TableName() const {
		return table_name;
	}

private:
	string connection_id;
	string schema_name;
	string table_name;
	unique_ptr<DataChunk> append_chunk;
};

class AppendResponseMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::APPEND_RESPONSE;

	explicit AppendResponseMessage() : QuackMessage(TYPE) {};

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<QuackMessage> Deserialize(Deserializer &deserializer);
};

class ErrorMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::ERROR;
	explicit ErrorMessage(const string &error_message_p) : QuackMessage(TYPE), error_message(error_message_p) {
	}
	const std::string &Error() const {
		return error_message;
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<QuackMessage> Deserialize(Deserializer &deserializer);

private:
	ErrorMessage() : QuackMessage(TYPE) {};
	string error_message;
};

} // namespace duckdb
