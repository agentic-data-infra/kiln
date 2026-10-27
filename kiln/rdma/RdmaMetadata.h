// The metadata calls (rpc::MetadataRpc) over rlib's RPC: serving a metadata
// server's state, and calling it. Built only with -c kiln.rdma=true.

#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "kiln/common/Types.h"
#include "kiln/rpc/MetadataRpc.h"

namespace kiln::rdma {

// Serves 'metadata' over RDMA until destroyed.
class RdmaMetadataServer {
 public:
  // Serves on the RDMA device of 'host' (empty for the first interface that
  // has one) at 'port' (0 for any free one). Throws Error(kUnavailable) if it
  // cannot.
  static std::unique_ptr<RdmaMetadataServer> start(
      std::shared_ptr<rpc::MetadataRpc> metadata,
      const std::string& host,
      uint16_t port);

  ~RdmaMetadataServer();

  // Where clients reach it (ClientOptions::rdmaMetadata).
  const Endpoint& endpoint() const {
    return endpoint_;
  }

 private:
  struct State;

  RdmaMetadataServer() = default;

  std::unique_ptr<State> state_;
  Endpoint endpoint_;
};

// The operations of the RDMA metadata server at 'endpoint'. Connects on the
// first call, and again after a call failed for the connection; calls fail
// with Error(kUnavailable) if the server cannot be reached, and with
// Error(kTooLarge) if a request or its response does not fit a message
// (kMaxMessageSize). Safe to use from any thread.
std::shared_ptr<rpc::MetadataRpc> connectRdmaMetadata(const Endpoint& endpoint);

} // namespace kiln::rdma
