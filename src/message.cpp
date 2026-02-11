#include "message.hpp"
#include <sys/socket.h>

using namespace duckdb;

string duckdb::MessageTypeToString(MessageType type) {
	switch (type) {
	case MessageType::BIND_REQUEST:
		return "BIND_REQUEST";
	case MessageType::BIND_RESPONSE:
		return "BIND_RESPONSE";
	case MessageType::EXECUTE_REQUEST:
		return "EXECUTE_REQUEST";
	case MessageType::EXECUTE_RESPONSE:
		return "EXECUTE_RESPONSE";
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

unique_ptr<ProtocolMessage> ProtocolMessage::FromSocket(int fd) {
	idx_t msg_len;
	MemoryStream read_stream;
	auto data_recv_header = recv(fd, &msg_len, sizeof(idx_t), MSG_WAITALL);
	if (data_recv_header != sizeof(idx_t)) {
		throw IOException("Failed to receive message length: %s", strerror(errno));
	}
	read_stream.GrowCapacity(msg_len);

	auto data_recv_message = recv(fd, (void *)read_stream.GetData(), msg_len, MSG_WAITALL);
	if (data_recv_message != msg_len) {
		throw IOException("Failed to receive message body (length %llu): %s", msg_len, strerror(errno));
	}
	return FromMemoryStream(read_stream);
}

void ProtocolMessage::ToSocket(int fd) const {
	MemoryStream write_stream;
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
	//	deserializer.Set<MessageType>(message_type); // TODO ??

	unique_ptr<ProtocolMessage> result;
	switch (message_type) {
	case MessageType::BIND_REQUEST:
		result = BindRequestMessage::Deserialize(deserializer);
		break;
	case MessageType::BIND_RESPONSE:
		result = BindResponseMessage::Deserialize(deserializer);
		break;
	case MessageType::EXECUTE_REQUEST:
		result = ExecuteRequestMessage::Deserialize(deserializer);
		break;
	case MessageType::EXECUTE_RESPONSE:
		result = ExecuteResponseMessage::Deserialize(deserializer);
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
	//	deserializer.Unset<MessageType>();
	return result;
}

void BindRequestMessage::Serialize(Serializer &serializer) const {
	ProtocolMessage::Serialize(serializer);
	serializer.WriteProperty<string>(200, "sql_query", sql_query);
}

unique_ptr<ProtocolMessage> BindRequestMessage::Deserialize(Deserializer &deserializer) {
	auto sql_query = deserializer.ReadProperty<string>(200, "sql_query");
	return make_uniq<BindRequestMessage>(sql_query);
}

void BindResponseMessage::Serialize(Serializer &serializer) const {
	ProtocolMessage::Serialize(serializer);
	serializer.WriteProperty<vector<LogicalType>>(210, "result_types", result_types);
	serializer.WriteProperty<vector<string>>(211, "result_names", result_names);
}

unique_ptr<ProtocolMessage> BindResponseMessage::Deserialize(Deserializer &deserializer) {
	auto result_types = deserializer.ReadProperty<vector<LogicalType>>(210, "result_types");
	auto result_names = deserializer.ReadProperty<vector<string>>(211, "result_names");
	return make_uniq<BindResponseMessage>(std::move(result_types), std::move(result_names));
}

void ExecuteRequestMessage::Serialize(Serializer &serializer) const {
	ProtocolMessage::Serialize(serializer);
	serializer.WriteProperty<string>(220, "sql_query", sql_query);
}

unique_ptr<ProtocolMessage> ExecuteRequestMessage::Deserialize(Deserializer &deserializer) {
	auto sql_query = deserializer.ReadProperty<string>(220, "sql_query");
	return make_uniq<ExecuteRequestMessage>(sql_query);
}

void ExecuteResponseMessage::Serialize(Serializer &serializer) const {
	ProtocolMessage::Serialize(serializer);
	// 230
}

unique_ptr<ProtocolMessage> ExecuteResponseMessage::Deserialize(Deserializer &deserializer) {
	auto result = make_uniq<ExecuteResponseMessage>();
	return std::move(result);
}

void FetchRequestMessage::Serialize(Serializer &serializer) const {
	ProtocolMessage::Serialize(serializer);
	// 240
}

unique_ptr<ProtocolMessage> FetchRequestMessage::Deserialize(Deserializer &deserializer) {
	auto result = make_uniq<FetchRequestMessage>();
	return std::move(result);
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
