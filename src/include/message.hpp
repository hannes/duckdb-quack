#pragma once

#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

namespace duckdb {

enum class MessageType : uint8_t {
	INVALID = 0,
	BIND_REQUEST = 1,
	BIND_RESPONSE = 2,
	EXECUTE_REQUEST = 3,
	EXECUTE_RESPONSE = 4,
	FETCH_REQUEST = 5,
	FETCH_RESPONSE = 6,
	ERROR = 100
};

string MessageTypeToString(MessageType type);

class ProtocolMessage {
public:
	void ToMemoryStream(MemoryStream &write_stream) const;
	static unique_ptr<ProtocolMessage> FromMemoryStream(MemoryStream &read_stream);

	void ToSocket(int fd) const;

	static unique_ptr<ProtocolMessage> FromSocket(int fd);

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
	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer);

	const MessageType &Type() const {
		return message_type;
	}

protected:
	ProtocolMessage(MessageType type) : message_type(type) {
	}
	MessageType message_type = MessageType::INVALID;
};

class BindRequestMessage : public ProtocolMessage {
public:
	static constexpr const MessageType TYPE = MessageType::BIND_REQUEST;

	BindRequestMessage(const string &sql_query_p) : ProtocolMessage(TYPE), sql_query(sql_query_p) {
	}
	const std::string Query() const {
		return sql_query;
	}
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer);

private:
	string sql_query; // FIXME do we want a string here
	BindRequestMessage() : ProtocolMessage(TYPE) {};
};

class BindResponseMessage : public ProtocolMessage {
public:
	static constexpr const MessageType TYPE = MessageType::BIND_RESPONSE;
	BindResponseMessage(const vector<LogicalType> &types_p, const vector<string> &names_p)
	    : ProtocolMessage(TYPE), result_types(types_p), result_names(names_p) {};

	const vector<LogicalType> &Types() const {
		return result_types;
	}

	const vector<string> &Names() const {
		return result_names;
	}
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer);

private:
	vector<LogicalType> result_types;
	vector<string> result_names;
};

class ExecuteRequestMessage : public ProtocolMessage {
public:
	static constexpr const MessageType TYPE = MessageType::EXECUTE_REQUEST;
	ExecuteRequestMessage(const string &sql_query_p) : ProtocolMessage(TYPE), sql_query(sql_query_p) {};
	const std::string Query() const {
		return sql_query;
	}
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer);

private:
	string sql_query; // FIXME we should not require this to be sent again
};

class ExecuteResponseMessage : public ProtocolMessage {
public:
	static constexpr const MessageType TYPE = MessageType::EXECUTE_RESPONSE;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer);
	ExecuteResponseMessage() : ProtocolMessage(TYPE) {};

	// TODO what does this message do? We return the query handle!!!
};

class FetchRequestMessage : public ProtocolMessage {
public:
	static constexpr const MessageType TYPE = MessageType::FETCH_REQUEST;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer);
	FetchRequestMessage() : ProtocolMessage(TYPE) {};
	// TODO what was this for again?
	// TODO contain the query ref
};

class FetchResponseMessage : public ProtocolMessage {
public:
	static constexpr const MessageType TYPE = MessageType::FETCH_RESPONSE;

	FetchResponseMessage(unique_ptr<DataChunk> response_data_p)
	    : ProtocolMessage(TYPE), response_data(std::move(response_data_p)) {};

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer);
	optional_ptr<DataChunk> ResponseData() {
		return response_data.get();
	}

private:
	unique_ptr<DataChunk> response_data;
	FetchResponseMessage() : ProtocolMessage(TYPE) {};
};

class ErrorMessage : public ProtocolMessage {
public:
	static constexpr const MessageType TYPE = MessageType::ERROR;
	ErrorMessage(string error_message_p) : ProtocolMessage(TYPE), error_message(error_message_p) {
	}
	const std::string Error() const {
		return error_message;
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer);

private:
	ErrorMessage() : ProtocolMessage(TYPE) {};
	string error_message;
};

} // namespace duckdb
