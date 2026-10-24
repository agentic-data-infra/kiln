// Kiln's RPC over fbthrift: serving the metadata and storage operations
// (MetadataRpc.h, StorageRpc.h) and calling them at a remote server.

#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "kiln/common/Types.h"
#include "kiln/rpc/MetadataRpc.h"
#include "kiln/rpc/StorageRpc.h"

namespace apache::thrift {
class ScopedServerInterfaceThread;
}

namespace kiln::rpc {

// A server answering calls in its own threads until destroyed.
class RpcServer {
 public:
  ~RpcServer();

  RpcServer(const RpcServer&) = delete;
  RpcServer& operator=(const RpcServer&) = delete;

  // The port it listens on; the one it was given, or the one the OS chose
  // for port 0.
  uint16_t port() const;

 private:
  friend std::unique_ptr<RpcServer> serveMetadata(
      std::shared_ptr<MetadataRpc>,
      const std::string&,
      uint16_t);
  friend std::unique_ptr<RpcServer> serveStorage(
      std::shared_ptr<StorageRpc>,
      const std::string&,
      uint16_t);

  explicit RpcServer(
      std::unique_ptr<apache::thrift::ScopedServerInterfaceThread> server);

  std::unique_ptr<apache::thrift::ScopedServerInterfaceThread> server_;
};

// Serves 'metadata' on 'host' (empty for all interfaces) and 'port' (0 for
// any free one). Throws Error(kUnavailable) if it cannot listen there.
std::unique_ptr<RpcServer> serveMetadata(
    std::shared_ptr<MetadataRpc> metadata,
    const std::string& host,
    uint16_t port);

std::unique_ptr<RpcServer> serveStorage(
    std::shared_ptr<StorageRpc> storage,
    const std::string& host,
    uint16_t port);

// The operations of the server at 'endpoint'. Throws Error(kUnavailable) if
// its host name does not resolve. Connects on the first call; calls fail
// with Error(kUnavailable) if the server cannot be reached. Safe to use from
// any thread.
std::shared_ptr<MetadataRpc> connectMetadata(const Endpoint& endpoint);

std::shared_ptr<StorageRpc> connectStorage(const Endpoint& endpoint);

// Calls the metadata server's ping, failing with Error(kUnavailable) if it
// cannot be reached.
folly::coro::Task<void> co_pingMetadata(const Endpoint& endpoint);

} // namespace kiln::rpc
