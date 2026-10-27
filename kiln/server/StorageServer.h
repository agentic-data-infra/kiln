#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <folly/coro/Task.h>

#include "kiln/common/Types.h"

namespace kiln::rpc {
class MetadataRpc;
class RpcServer;
}

namespace kiln::server {

class RegionStore;
class RdmaRegionServer;

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
  // DRAM: where this server's new directory of region files goes; empty for
  // /dev/shm. Local FS: this server's own directory, created if missing,
  // whose region files stay when the server stops and are reattached with
  // their data when it starts again; empty for a new directory in $TMPDIR,
  // else /tmp, that goes when it stops.
  std::string dir;
  // How long to keep trying to reach the metadata server.
  std::chrono::milliseconds connectTimeout{std::chrono::seconds(60)};
  // How often to register again, so that a restarted metadata server learns
  // of the regions.
  std::chrono::milliseconds registerInterval{std::chrono::seconds(5)};
  // Also serve DRAM regions for one-sided RDMA (RdmaAccess), on the RDMA
  // device of 'rdmaHost' (empty for the first interface that has one). Needs
  // a build with -c kiln.rdma=true, a device, and a locked memory limit
  // (ulimit -l) that fits the regions; without them the server serves TCP
  // only, with a warning. Local-FS regions are served over TCP only.
  bool rdma{false};
  std::string rdmaHost;
};

// A storage server: holds the blocks of one tier in its regions and serves
// reads and writes of them. It knows nothing of chunks; the metadata server
// hands out its blocks. A DRAM server's data is gone when it stops; a
// local-FS server with its own directory keeps it.
class StorageServer {
 public:
  // Creates (or reattaches) the regions, serves them and registers them
  // with the metadata server. Fails with kiln::Error.
  static folly::coro::Task<std::unique_ptr<StorageServer>> co_start(
      StorageServerOptions options);

  // Stops serving and removes the regions, unless kept.
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

  // Registers again every registerInterval until stopped.
  void reregisterLoop();

  StorageServerOptions options_;
  std::string dir_;
  // Whether dir_ and the region files stay.
  bool keep_{false};
  std::shared_ptr<rpc::MetadataRpc> metadata_;
  std::thread reregister_;
  std::mutex mutex_;
  std::condition_variable stop_;
  bool stopping_{false};
  std::shared_ptr<RegionStore> store_;
  // Set if serving RDMA; goes before the regions.
  std::unique_ptr<RdmaRegionServer> rdma_;
  std::unique_ptr<rpc::RpcServer> rpcServer_;
  Endpoint endpoint_;
  std::vector<RegionInfo> regions_;
};

} // namespace kiln::server
