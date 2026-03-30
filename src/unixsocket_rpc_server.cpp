#include "rpc_server.hpp"
#include "message.hpp"

#include "duckdb/main/connection.hpp"
#include "duckdb/common/render_tree.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/stat.h>

using namespace duckdb;

UnixSocketRpcServer::~UnixSocketRpcServer() {
	// this should interrupt accept() in the listen thread
	close(unix_socket_server_fd);
	unix_socket_keep_listening = false;
	try {
		// Wait for all threads in the pool to exit.
		for (idx_t i = 0; i < listen_threads.size(); ++i) {
			listen_threads[i].join();
		}
	} catch (std::exception &) {
	}
}

void UnixSocketRpcServer::UnixSocketListenThread(UnixSocketRpcServer *rpc_server) {
	D_ASSERT(rpc_server);

	while (rpc_server->unix_socket_keep_listening) {
		unsigned int sock_len = 0;
		auto client_socket_fd = accept(rpc_server->unix_socket_server_fd,
		                               reinterpret_cast<sockaddr *>(&rpc_server->unix_socket_address), &sock_len);
		if (client_socket_fd == -1) {
			continue;
		}

		std::thread accept_thread([rpc_server, client_socket_fd] {
			bool open = true;
			MemoryStream read_stream, write_stream;

			do {
				try {
					auto received_message = ProtocolMessage::FromSocket(client_socket_fd, read_stream);
					// printf("S RECV %s\n", MessageTypeToString(received_message->Type()).c_str());
					auto response_message = rpc_server->HandleMessage(*received_message);
					// printf("S SEND %s\n", MessageTypeToString(response_message->Type()).c_str());

					response_message->ToSocket(client_socket_fd, write_stream);
				} catch (IOException &) {
					open = false;
				}

			} while (open);
			close(client_socket_fd);
		});
		accept_thread.detach(); // TODO do we need this?
	}
	// TODO clean up socket?
}

void UnixSocketRpcServer::Listen(const string &listen_string_p) {
	if (listen_string_p.empty()) {
		throw InvalidInputException("Empty listen string specified");
	}
	D_ASSERT(!StringUtil::StartsWith(listen_string_p, "wss:"));

	unix_socket_server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (unix_socket_server_fd == -1) {
		throw IOException("Error creating socket");
	}

	unix_socket_address.sun_family = AF_UNIX;
	memset(&unix_socket_address, 0, sizeof(sockaddr_un));
	strncpy(unix_socket_address.sun_path, listen_string_p.c_str(), sizeof(unix_socket_address.sun_path) - 1);

	auto unlink_result = unlink(unix_socket_address.sun_path);
	if (unlink_result && errno != ENOENT) {
		throw IOException("Error cleaning up socket %s: %s", listen_string_p, strerror(errno));
	}

	if (bind(unix_socket_server_fd, reinterpret_cast<sockaddr *>(&unix_socket_address),
	         SUN_LEN(&unix_socket_address)) ||
	    chmod(listen_string_p.c_str(), S_IWUSR | S_IRUSR) ||
	    listen(unix_socket_server_fd, 100 /* TODO: magic constant for connect queue length, should be fine */)) {
		throw IOException("Error listening to socket %s: %s", listen_string_p, strerror(errno));
	}

	unix_socket_keep_listening = true;
	listen_threads.push_back(std::thread(UnixSocketListenThread, this));
}
