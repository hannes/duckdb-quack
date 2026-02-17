#include "message.hpp"
#include <sys/socket.h>

using namespace duckdb;

string duckdb::MessageTypeToString(MessageType type) {
	switch (type) {
	case MessageType::CONNECTION_REQUEST:
		return "CONNECTION_REQUEST";
	case MessageType::CONNECTION_RESPONSE:
		return "CONNECTION_RESPONSE";
	case MessageType::PREPARE_REQUEST:
		return "PREPARE_REQUEST";
	case MessageType::PREPARE_RESPONSE:
		return "PREPARE_RESPONSE";
	case MessageType::FETCH_REQUEST:
		return "FETCH_REQUEST";
	case MessageType::FETCH_RESPONSE:
		return "FETCH_RESPONSE";
	case MessageType::ERROR:
		return "ERROR";
	case MessageType::INVALID:
		break;
	}
	return "INVALID";
}

void ProtocolMessage::ToMemoryStream(MemoryStream &write_stream) const {
	write_stream.Rewind();
	SerializationOptions options;
	options.serialization_compatibility = SerializationCompatibility::FromIndex(10);
	BinarySerializer serializer(write_stream, options);

	serializer.Begin();
	Serialize(serializer);
	serializer.End();
}

unique_ptr<ProtocolMessage> ProtocolMessage::FromMemoryStream(MemoryStream &read_stream) {
	read_stream.Rewind();
	BinaryDeserializer deserializer(read_stream);
	return Deserialize(deserializer);
}

unique_ptr<ProtocolMessage> ProtocolMessage::FromSocket(int fd, MemoryStream &read_stream) {
	idx_t msg_len;
	auto data_recv_header = recv(fd, &msg_len, sizeof(idx_t), MSG_WAITALL);
	if (data_recv_header != sizeof(idx_t)) {
		throw IOException("Failed to receive message length: %s", strerror(errno));
	}
	read_stream.Rewind();
	read_stream.GrowCapacity(msg_len);

	auto data_recv_message = recv(fd, (void *)read_stream.GetData(), msg_len, MSG_WAITALL);
	if (data_recv_message != msg_len) {
		throw IOException("Failed to receive message body (length %llu): %s", msg_len, strerror(errno));
	}
	return FromMemoryStream(read_stream);
}

void ProtocolMessage::ToSocket(int fd, MemoryStream &write_stream) const {
	write_stream.Rewind();
	ToMemoryStream(write_stream);

	idx_t msg_len = write_stream.GetPosition();
	if (send(fd, &msg_len, sizeof(idx_t), 0) != sizeof(idx_t)) {
		throw IOException("Failed to send message length (%llu): %s", msg_len, strerror(errno));
	}
	if (send(fd, write_stream.GetData(), msg_len, 0) != msg_len) {
		throw IOException("Failed to send message body (length %llu): %s", msg_len, strerror(errno));
	}
}

void ProtocolMessage::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<uint8_t>(100, "message_type", static_cast<uint8_t>(message_type));
}

unique_ptr<ProtocolMessage> ProtocolMessage::Deserialize(Deserializer &deserializer) {
	auto message_type = static_cast<MessageType>(deserializer.ReadProperty<uint8_t>(100, "message_type"));

	unique_ptr<ProtocolMessage> result;

	switch (message_type) {
	case MessageType::CONNECTION_REQUEST:
		result = ConnectionRequestMessage::Deserialize(deserializer);
		break;
	case MessageType::CONNECTION_RESPONSE:
		result = ConnectionResponseMessage::Deserialize(deserializer);
		break;
	case MessageType::PREPARE_REQUEST:
		result = PrepareRequestMessage::Deserialize(deserializer);
		break;
	case MessageType::PREPARE_RESPONSE:
		result = PrepareResponseMessage::Deserialize(deserializer);
		break;
	case MessageType::FETCH_REQUEST:
		result = FetchRequestMessage::Deserialize(deserializer);
		break;
	case MessageType::FETCH_RESPONSE:
		result = FetchResponseMessage::Deserialize(deserializer);
		break;
	case MessageType::ERROR:
		result = ErrorMessage::Deserialize(deserializer);
		break;
	default:
		throw SerializationException("Unsupported type for deserialization of Message!");
	}

	return result;
}

void ConnectionRequestMessage::Serialize(Serializer &serializer) const {
	ProtocolMessage::Serialize(serializer);
}

unique_ptr<ProtocolMessage> ConnectionRequestMessage::Deserialize(Deserializer &deserializer) {
	return make_uniq<ConnectionRequestMessage>();
}

void ConnectionResponseMessage::Serialize(Serializer &serializer) const {
	ProtocolMessage::Serialize(serializer);
	serializer.WriteProperty<string>(101, "connection_id", connection_id);
}

unique_ptr<ProtocolMessage> ConnectionResponseMessage::Deserialize(Deserializer &deserializer) {
	auto connection_id = deserializer.ReadProperty<string>(101, "connection_id");
	return make_uniq<ConnectionResponseMessage>(connection_id);
}

void PrepareRequestMessage::Serialize(Serializer &serializer) const {
	ProtocolMessage::Serialize(serializer);
	serializer.WriteProperty<string>(101, "connection_id", connection_id);
	serializer.WriteProperty<string>(200, "sql_query", sql_query);
}

unique_ptr<ProtocolMessage> PrepareRequestMessage::Deserialize(Deserializer &deserializer) {
	auto connection_id = deserializer.ReadProperty<string>(101, "connection_id");
	auto sql_query = deserializer.ReadProperty<string>(200, "sql_query");
	return make_uniq<PrepareRequestMessage>(connection_id, sql_query);
}

void PrepareResponseMessage::Serialize(Serializer &serializer) const {
	ProtocolMessage::Serialize(serializer);
	serializer.WriteProperty<vector<LogicalType>>(210, "result_types", result_types);
	serializer.WriteProperty<vector<string>>(211, "result_names", result_names);
	serializer.WriteProperty<optional_idx>(212, "estimated_cardinality", estimated_cardinality);
}

unique_ptr<ProtocolMessage> PrepareResponseMessage::Deserialize(Deserializer &deserializer) {
	auto result_types = deserializer.ReadProperty<vector<LogicalType>>(210, "result_types");
	auto result_names = deserializer.ReadProperty<vector<string>>(211, "result_names");
	auto estimated_cardinality = deserializer.ReadProperty<optional_idx>(212, "estimated_cardinality");
	return make_uniq<PrepareResponseMessage>(std::move(result_types), std::move(result_names), estimated_cardinality);
}

void FetchRequestMessage::Serialize(Serializer &serializer) const {
	ProtocolMessage::Serialize(serializer);
	serializer.WriteProperty<string>(101, "connection_id", connection_id);

	// 240
}

unique_ptr<ProtocolMessage> FetchRequestMessage::Deserialize(Deserializer &deserializer) {
	auto connection_id = deserializer.ReadProperty<string>(101, "connection_id");
	return make_uniq<FetchRequestMessage>(connection_id);
}

void FetchResponseMessage::Serialize(Serializer &serializer) const {
	ProtocolMessage::Serialize(serializer);
	auto response_data_present = response_data && response_data->size() > 0;
	serializer.WriteProperty<bool>(250, "response_data_present", response_data_present);
	if (response_data_present) {
		serializer.WriteObject(251, "response_data",
		                       [&](Serializer &inner_serializer) { response_data->Serialize(inner_serializer); });
	}
}

unique_ptr<ProtocolMessage> FetchResponseMessage::Deserialize(Deserializer &deserializer) {
	auto response_data_present = deserializer.ReadProperty<bool>(250, "response_data_present");
	if (!response_data_present) {
		return make_uniq<FetchResponseMessage>(nullptr);
	}

	auto result_chunk = make_uniq<DataChunk>();
	deserializer.ReadObject(251, "response_data",
	                        [&](Deserializer &inner_deserializer) { result_chunk->Deserialize(inner_deserializer); });
	return make_uniq<FetchResponseMessage>(std::move(result_chunk));
}

void ErrorMessage::Serialize(Serializer &serializer) const {
	ProtocolMessage::Serialize(serializer);
	serializer.WriteProperty<string>(260, "error_message", error_message);
}

unique_ptr<ProtocolMessage> ErrorMessage::Deserialize(Deserializer &deserializer) {
	auto error_message = deserializer.ReadProperty<string>(260, "error_message");
	return make_uniq<ErrorMessage>(error_message);
}
