#pragma once

#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

namespace duckdb {

enum class MessageType : uint8_t {
	INVALID = 0,
	BIND = 1,
	BIND_RESULT = 2,
	EXECUTE = 3,
	EXECUTE_RESULT = 4,
	FETCH = 5,
	FETCH_RESULT = 6,
	FETCH_DONE = 7,
	ERROR = 100
};

struct ProtocolMessage {

	unique_ptr<MemoryStream> ToMemoryStream();
	static unique_ptr<ProtocolMessage> FromPayload(std::string const &payload);

	void Serialize(Serializer &serializer);
	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer);

	MessageType type;
	string query;
	string error;
	vector<string> names;
	vector<LogicalType> types;
	unique_ptr<DataChunk> data;
};

} // namespace duckdb
