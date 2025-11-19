#include "message.hpp"
using namespace duckdb;

void ProtocolMessage::Serialize(Serializer &serializer) {

	serializer.WriteProperty<uint8_t>(1, "type", static_cast<uint8_t>(type));
	serializer.WriteProperty<string>(2, "query", query);
	serializer.WriteProperty<string>(3, "error", error);
	serializer.WriteProperty<vector<LogicalType>>(4, "types", types);
	serializer.WriteProperty<vector<string>>(5, "names", names);
	if (type == MessageType::FETCH_RESULT) {
		serializer.WriteObject(6, "data", [&](Serializer &serializer2) { data->Serialize(serializer2); });
	}
}

unique_ptr<ProtocolMessage> ProtocolMessage::Deserialize(Deserializer &deserializer) {
	auto result = make_uniq<ProtocolMessage>();
	result->type = static_cast<MessageType>(deserializer.ReadProperty<uint8_t>(1, "type"));
	result->query = deserializer.ReadProperty<string>(2, "query");
	result->error = deserializer.ReadProperty<string>(3, "error");
	result->types = deserializer.ReadProperty<vector<LogicalType>>(4, "types");
	result->names = deserializer.ReadProperty<vector<string>>(5, "names");
	if (result->type == MessageType::FETCH_RESULT) {
		result->data = make_uniq<DataChunk>();
		deserializer.ReadObject(6, "data",
		                        [&](Deserializer &deserializer2) { result->data->Deserialize(deserializer2); });
	}
	return result;
}

unique_ptr<MemoryStream> ProtocolMessage::ToMemoryStream() {
	auto write_stream = make_uniq<MemoryStream>(); // TODO pass allocator here
	BinarySerializer serializer(*write_stream);
	serializer.Begin();
	Serialize(serializer);
	serializer.End();
	return write_stream;
}

unique_ptr<ProtocolMessage> ProtocolMessage::FromPayload(std::string const &payload) {
	MemoryStream read_stream(data_ptr_cast((void *)payload.data()), payload.size());
	BinaryDeserializer deserializer(read_stream);
	return Deserialize(deserializer);
}
