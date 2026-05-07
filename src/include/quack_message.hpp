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
	APPEND_REQUEST = 9,
	APPEND_RESPONSE = 10,
	ERROR_RESPONSE = 100
};

template <>
const char *EnumUtil::ToChars<MessageType>(MessageType value);
template <>
MessageType EnumUtil::FromString<MessageType>(const char *value);

// workaround for wrong serialization functions signature on DataChunk :/
// TODO: remove in 2.0
class DataChunkWrapper {
public:
	DataChunkWrapper(DataChunk &chunk_p) {
		chunk.InitializeEmpty(chunk_p.GetTypes());
		chunk.Reference(chunk_p);
	}
	DataChunk &Chunk() {
		return chunk;
	}
	void Serialize(Serializer &serializer) const;
	static unique_ptr<DataChunkWrapper> Deserialize(Deserializer &deserializer);

private:
	DataChunk chunk;
};

string MessageTypeToString(MessageType type);

class QuackMessage {
public:
	void ToMemoryStream(MemoryStream &write_stream) const;
	static unique_ptr<QuackMessage> FromMemoryStream(MemoryStream &read_stream);

	template <class TARGET>
	TARGET &Cast() {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast message to type - message type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast message to type - message type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}

	virtual void Serialize(Serializer &serializer) const;
	static unique_ptr<QuackMessage> Deserialize(Deserializer &deserializer);

	const MessageType &Type() const {
		return type;
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
	explicit QuackMessage(MessageType type) : type(type) {
	}
	MessageType type = MessageType::INVALID;
	optional_idx client_query_id;
};

class PrepareRequestMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::PREPARE_REQUEST;

	PrepareRequestMessage(const string &connection_id_p, const string &sql_query_p)
	    : QuackMessage(TYPE), connection_id(connection_id_p), sql_query(sql_query_p) {
	}
	const std::string &Query() const {
		return sql_query;
	}
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<QuackMessage> Deserialize(Deserializer &deserializer);

	const std::string &ConnectionId() const {
		return connection_id;
	}

private:
	string connection_id; // FIXME abstract this to some superclass
	string sql_query;
};

class PrepareResponseMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::PREPARE_RESPONSE;

	PrepareResponseMessage(const vector<LogicalType> &types_p, const vector<string> &names_p,
	                       vector<unique_ptr<DataChunkWrapper>> results_p, bool needs_more_fetch_p)
	    : QuackMessage(TYPE), result_types(types_p), result_names(names_p), results(std::move(results_p)),
	      needs_more_fetch(needs_more_fetch_p) {
	}

	const vector<LogicalType> &Types() const {
		return result_types;
	}

	const vector<string> &Names() const {
		return result_names;
	}

	vector<unique_ptr<DataChunkWrapper>> &MutableResults() {
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
	vector<unique_ptr<DataChunkWrapper>> results;
	bool needs_more_fetch;
};

// TODO this is where auth goes
class ConnectionRequestMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::CONNECTION_REQUEST;

	explicit ConnectionRequestMessage(const string &auth_string_p) : QuackMessage(TYPE), auth_string(auth_string_p) {
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

private:
	string connection_id;
};

class FetchResponseMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::FETCH_RESPONSE;

	FetchResponseMessage() : QuackMessage(TYPE) {};
	explicit FetchResponseMessage(vector<unique_ptr<DataChunkWrapper>> results_p)
	    : QuackMessage(TYPE), results(std::move(results_p)) {};
	FetchResponseMessage(vector<unique_ptr<DataChunkWrapper>> results_p, optional_idx batch_index_p)
	    : QuackMessage(TYPE), results(std::move(results_p)), batch_index(batch_index_p) {};

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<QuackMessage> Deserialize(Deserializer &deserializer);

	vector<unique_ptr<DataChunkWrapper>> &MutableResults() {
		return results;
	}

	optional_idx BatchIndex() const {
		return batch_index;
	}

private:
	vector<unique_ptr<DataChunkWrapper>> results;
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

class AppendRequestMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::APPEND_REQUEST;

	explicit AppendRequestMessage(const string &connection_id_p, const string &schema_name_p,
	                              const string &table_name_p, unique_ptr<DataChunkWrapper> append_chunk_p)
	    : QuackMessage(TYPE), connection_id(connection_id_p), schema_name(schema_name_p), table_name(table_name_p),
	      append_chunk(std::move(append_chunk_p)) {};

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<QuackMessage> Deserialize(Deserializer &deserializer);
	DataChunk &AppendChunk() const {
		return append_chunk->Chunk();
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
	unique_ptr<DataChunkWrapper> append_chunk;
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
	static constexpr MessageType TYPE = MessageType::ERROR_RESPONSE;
	explicit ErrorMessage(const string &message_p) : QuackMessage(TYPE), message(message_p) {
	}
	const std::string &Error() const {
		return message;
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<QuackMessage> Deserialize(Deserializer &deserializer);

private:
	ErrorMessage() : QuackMessage(TYPE) {};
	string message;
};

} // namespace duckdb
