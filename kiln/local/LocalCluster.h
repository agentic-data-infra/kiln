#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "kiln/common/Types.h"
#include "kiln/meta/Metadata.h"
#include "kiln/server/StorageServer.h"

namespace kiln::rpc {
class RpcServer;
}

namespace kiln::rdma {
class RdmaMetadataServer;
}

namespace kiln::local {

struct LocalClusterOptions {
  uint32_t dramServers{2};
  uint32_t localFsServers{2};
  uint32_t regionsPerServer{1};
  uint64_t regionSize{64 << 20};
  // Each storage server's budget; 0 for all of its regions.
  uint64_t budget{0};
  meta::MetadataOptions metadata;
  // Where local-FS regions go; empty for the storage server's default.
  std::string localFsDir;
  // The metadata server and DRAM servers also serve RDMA (kiln_meta --rdma,
  // StorageServerOptions::rdma), on the first interface with an RDMA device.
  bool rdma{false};
};

// A metadata server and storage servers in this process, for tests,
// examples and benchmarks. They serve on 127.0.0.1 like separate processes
// would (RDMA on the RDMA device's address), so clients use them as they
// would a real cluster.
class LocalCluster {
 public:
  explicit LocalCluster(LocalClusterOptions options = {});

  ~LocalCluster();

  // The metadata server, for ClientOptions::metadata.
  const Endpoint& metadataEndpoint() const {
    return metadataEndpoint_;
  }

  // The metadata server's RDMA endpoint, for ClientOptions::rdmaMetadata;
  // unset without the rdma option, an RDMA build or an RDMA device.
  const std::optional<Endpoint>& rdmaMetadataEndpoint() const {
    return rdmaMetadataEndpoint_;
  }

  // The metadata server's state, e.g. to check free blocks in tests.
  meta::Metadata& metadata() const {
    return *metadata_;
  }

  const std::vector<std::unique_ptr<server::StorageServer>>& servers() const {
    return servers_;
  }

 private:
  std::shared_ptr<meta::Metadata> metadata_;
  std::unique_ptr<rpc::RpcServer> metadataServer_;
  Endpoint metadataEndpoint_;
  std::unique_ptr<rdma::RdmaMetadataServer> rdmaMetadataServer_;
  std::optional<Endpoint> rdmaMetadataEndpoint_;
  std::vector<std::unique_ptr<server::StorageServer>> servers_;
};

} // namespace kiln::local
