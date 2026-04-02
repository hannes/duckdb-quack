#pragma once
#include "duckdb/common/string_util.hpp"

namespace duckdb {

class RpcUri {
public:
	RpcUri() : RpcUri("remote:localhost") {
	} // orrr

	RpcUri(string uri_p, bool ssl_p = true) : ssl(ssl_p), uri(uri_p) {
		// we should really instantiate a parser here instead, but alas
		// whitespace be gone
		ipv6 = false;
		port = 1294;

		StringUtil::Trim(uri);
		// first off, lets be tolerant and accept this variant, too
		if (StringUtil::StartsWith(uri, "remote://")) {
			uri = StringUtil::Replace(uri, "remote://", "remote:");
		}
		if (!StringUtil::StartsWith(uri, "remote:")) {
			throw InvalidInputException("Invalid DuckDB remote URI, needs to start with 'remote:'");
		}

		auto remainder = StringUtil::Replace(uri, "remote:", "");
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
	bool ssl;
	bool ipv6;
	string host;
	uint16_t port; // default port!
	string http;
	string uri;
};

} // namespace duckdb
