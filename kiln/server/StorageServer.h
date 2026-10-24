#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <folly/coro/Task.h>

#include "kiln/common/Types.h"

namespace kiln::rpc {
class RpcServer;
}

namespace kiln::server {

class RegionStore;

struct StorageServerOptions {
  // The metadata server to register with.
  Endpoint metadata;
  // The interface to serve on; empty for all.
  std::string bindHost;
  // The host name clients reach this server by; empty for localHostName().
  std::string host;
  // 0 for any free port.
  uint16_t port{0};
  Tier tier{Tier::kDram};
  uint32_t regions{1};
  // A multiple of kBlockSize.
  uint64_t regionSize{kRegionSize};
  // Bytes of the regions the metadata server may hand out; 0 for all.
  uint64_t budget{0};
  // Where this server's directory of region files goes; empty for /dev/shm
  // (DRAM) or $TMPDIR, else /tmp (local FS).
  std::string dir;
  // How long to keep trying to reach the metadata server.
  std::chrono::milliseconds connectTimeout{std::chrono::seconds(60)};
};

// A storage server: holds the blocks of one tier in its regions and serves
// reads and writes of them. It knows nothing of chunks; the metadata server
// hands out its blocks. Its data is gone when it stops.
class StorageServer {
 public:
  // Creates the regions in a new directory, serves them and registers them
  // with the metadata server. Fails with kiln::Error.
  static folly::coro::Task<std::unique_ptr<StorageServer>> co_start(
      StorageServerOptions options);

  // Stops serving and removes the regions.
  ~StorageServer();

  // This server's endpoint, as clients reach it.
  const Endpoint& endpoint() const {
    return endpoint_;
  }

  // The regions, as registered with the metadata server.
  const std::vector<RegionInfo>& regions() const {
    return regions_;
  }

 private:
  StorageServer() = default;

  std::string dir_;
  std::shared_ptr<RegionStore> store_;
  std::unique_ptr<rpc::RpcServer> rpcServer_;
  Endpoint endpoint_;
  std::vector<RegionInfo> regions_;
};

} // namespace kiln::server
