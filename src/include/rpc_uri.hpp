#pragma once
#include "duckdb/common/string_util.hpp"

// 1294 default port? seems to be unused

namespace duckdb {

class RpcUri {
public:
	RpcUri(string uri_p, bool ssl_p = true) : ssl(ssl_p), uri(uri_p) {
		// we should really instantiate a parser here instead, but alas
		// whitespace be gone
		StringUtil::Trim(uri);
		// first off, lets be tolerant and accept this variant, too
		if (StringUtil::StartsWith(uri, "quack://")) {
			uri = StringUtil::Replace(uri, "quack://", "quack:");
		}
		if (!StringUtil::StartsWith(uri, "quack:")) {
			throw InvalidInputException("Invalid DuckDB Quack RPC URI, needs to start with 'quack:'");
		}

		auto remainder = StringUtil::Replace(uri, "quack:", "");
		if (remainder.empty()) {
			throw InvalidInputException("Missing hostname");
		}
		// we have an ipv6 URL
		if (StringUtil::StartsWith(remainder, "[")) {
			if (!StringUtil::Contains(remainder, ']')) {
				throw InvalidInputException("Invalid IPv6 URL, missing ']'");
			}
			ipv6 = true;
			auto pos = remainder.find(']');
			host = remainder.substr(1, pos - 1);
			if (host.empty()) {
				throw InvalidInputException("Missing IPv6 Address");
			}
			remainder = remainder.substr(pos + 1);
		}
		// a port was specified
		if (StringUtil::Contains(remainder, ':')) {
			auto pos = remainder.find(':');
			auto port_str = remainder.substr(pos + 1);
			if (port_str.empty()) {
				throw InvalidInputException("Invalid Port");
			}
			int raw_port;
			try {
				raw_port = stoi(port_str);
			} catch (std::exception &) {
				throw InvalidInputException("Invalid Port");
			}
			if (raw_port < 1 || raw_port > 65535) {
				throw InvalidInputException("Invalid Port");
			}
			port = raw_port;
			remainder = remainder.substr(0, pos);
		}
		// this should be it
		if (!ipv6) {
			host = remainder;
		}
		http = StringUtil::Format("http%s://%s:%d", ssl ? "s" : "", ipv6 ? "[" + host + "]" : host, port);
	}
	string Http() const {
		return http;
	}
	string Uri() const {
		return uri;
	}
	string Host() const {
		return host;
	}
	uint16_t Port() const {
		return port;
	}
	bool Ssl() const {
		return ssl;
	}
	bool IPv6() const {
		return ipv6;
	}

private:
	bool ssl = true;
	bool ipv6 = false;
	string host = "localhost";
	uint16_t port = 1294; // default port!
	string http;
	string uri;
};

} // namespace duckdb
