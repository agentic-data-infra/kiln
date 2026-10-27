#include "kiln/local/LocalCluster.h"

#include <folly/coro/BlockingWait.h>

#include "kiln/rpc/Rpc.h"
#if KILN_RDMA
#include <glog/logging.h>

#include "kiln/common/Error.h"
#include "kiln/rdma/RdmaMetadata.h"
#else
namespace kiln::rdma {
// Never made without RDMA.
class RdmaMetadataServer {};
} // namespace kiln::rdma
#endif

namespace kiln::local {

LocalCluster::LocalCluster(LocalClusterOptions options)
    : metadata_{std::make_shared<meta::Metadata>(options.metadata)},
      metadataServer_{rpc::serveMetadata(metadata_, "127.0.0.1", 0)},
      metadataEndpoint_{"127.0.0.1", metadataServer_->port()},
      metadataAddress_{metadataEndpoint_} {
#if KILN_RDMA
  if (options.rdma) {
    try {
      rdmaMetadataServer_ = rdma::RdmaMetadataServer::start(metadata_, "", 0);
      metadataAddress_.rdma = rdmaMetadataServer_->endpoint();
    } catch (const Error& e) {
      LOG(WARNING) << e.what() << "; metadata over TCP only";
    }
  }
#endif
  auto start = [&](Tier tier, const std::string& dir) {
    servers_.push_back(folly::coro::blockingWait(
        server::StorageServer::co_start(server::StorageServerOptions{
            .metadata = metadataEndpoint_,
            .bindHost = "127.0.0.1",
            .host = "127.0.0.1",
            .tier = tier,
            .regions = options.regionsPerServer,
            .regionSize = options.regionSize,
            .budget = options.budget,
            .dir = dir,
            .rdma = options.rdma && tier == Tier::kDram})));
  };
  for (uint32_t i = 0; i < options.dramServers; ++i) {
    start(Tier::kDram, "");
  }
  for (uint32_t i = 0; i < options.localFsServers; ++i) {
    start(Tier::kLocalFs, options.localFsDir);
  }
}

LocalCluster::~LocalCluster() {
  servers_.clear();
  rdmaMetadataServer_.reset();
  metadataServer_.reset();
}

} // namespace kiln::local
