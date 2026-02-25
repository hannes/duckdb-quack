#pragma once

#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

namespace duckdb {

enum class MessageType : uint8_t {
	INVALID = 0,
	CONNECTION_REQUEST = 1,
	CONNECTION_RESPONSE = 2,
	PREPARE_REQUEST = 3,
	PREPARE_RESPONSE = 4,
	FETCH_REQUEST = 7,
	FETCH_RESPONSE = 8,
	ERROR = 100
};

string MessageTypeToString(MessageType type);

class ProtocolMessage {
public:
	void ToMemoryStream(MemoryStream &write_stream) const;
	static unique_ptr<ProtocolMessage> FromMemoryStream(MemoryStream &read_stream);

	void ToSocket(int fd, MemoryStream &write_stream) const;

	static unique_ptr<ProtocolMessage> FromSocket(int fd, MemoryStream &read_stream);

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

	virtual ~ProtocolMessage() {
	}

protected:
	explicit ProtocolMessage(MessageType type) : message_type(type) {
	}
	MessageType message_type = MessageType::INVALID;
};

class PrepareRequestMessage : public ProtocolMessage {
public:
	static constexpr MessageType TYPE = MessageType::PREPARE_REQUEST;

	PrepareRequestMessage(const string &connection_id_p, const string &sql_query_p, bool immediately_execute_p = false)
	    : ProtocolMessage(TYPE), connection_id(connection_id_p), sql_query(sql_query_p),
	      immediately_execute(immediately_execute_p) {
	}
	const std::string &Query() const {
		return sql_query;
	}
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer);

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

	PrepareRequestMessage() : ProtocolMessage(TYPE) {};
};

class PrepareResponseMessage : public ProtocolMessage {
public:
	static constexpr MessageType TYPE = MessageType::PREPARE_RESPONSE;
	PrepareResponseMessage(const vector<LogicalType> &types_p, const vector<string> &names_p,
	                       optional_idx estimated_cardinality_p)
	    : ProtocolMessage(TYPE), result_types(types_p), result_names(names_p),
	      estimated_cardinality(estimated_cardinality_p) {};

	const vector<LogicalType> &Types() const {
		return result_types;
	}

	const vector<string> &Names() const {
		return result_names;
	}
	optional_idx EstimatedCardinality() const {
		return estimated_cardinality;
	}
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer);

private:
	vector<LogicalType> result_types;
	vector<string> result_names;
	optional_idx estimated_cardinality;
};

// TODO this is where auth goes
class ConnectionRequestMessage : public ProtocolMessage {
public:
	static constexpr MessageType TYPE = MessageType::CONNECTION_REQUEST;

	ConnectionRequestMessage() : ProtocolMessage(TYPE) {
	}
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer);
};

class ConnectionResponseMessage : public ProtocolMessage {
public:
	static constexpr MessageType TYPE = MessageType::CONNECTION_RESPONSE;

	explicit ConnectionResponseMessage(const string &connection_id_p)
	    : ProtocolMessage(TYPE), connection_id(connection_id_p) {
	}

	const std::string &ConnectionId() const {
		return connection_id;
	}
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer);

private:
	ConnectionResponseMessage() : ProtocolMessage(TYPE) {
	}
	string connection_id;
};

class FetchRequestMessage : public ProtocolMessage {
public:
	static constexpr MessageType TYPE = MessageType::FETCH_REQUEST;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer);
	const std::string &ConnectionId() const {
		return connection_id;
	}
	explicit FetchRequestMessage(const string &connection_id_p)
	    : ProtocolMessage(TYPE), connection_id(connection_id_p) {};

	// TODO what was this for again?
	// TODO contain the query ref
private:
	string connection_id;
};

class FetchResponseMessage : public ProtocolMessage {
public:
	static constexpr MessageType TYPE = MessageType::FETCH_RESPONSE;

	explicit FetchResponseMessage(unique_ptr<DataChunk> response_data_p)
	    : ProtocolMessage(TYPE), response_data(std::move(response_data_p)) {};

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer);
	optional_ptr<DataChunk> ResponseData() const {
		return response_data.get();
	}

private:
	unique_ptr<DataChunk> response_data;
	FetchResponseMessage() : ProtocolMessage(TYPE) {};
};

class ErrorMessage : public ProtocolMessage {
public:
	static constexpr MessageType TYPE = MessageType::ERROR;
	explicit ErrorMessage(const string &error_message_p) : ProtocolMessage(TYPE), error_message(error_message_p) {
	}
	const std::string &Error() const {
		return error_message;
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer);

private:
	ErrorMessage() : ProtocolMessage(TYPE) {};
	string error_message;
};

} // namespace duckdb
