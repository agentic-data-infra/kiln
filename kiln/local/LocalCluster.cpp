#include "kiln/local/LocalCluster.h"

#include <folly/coro/BlockingWait.h>

#include "kiln/rpc/Rpc.h"

namespace kiln::local {

LocalCluster::LocalCluster(LocalClusterOptions options)
    : metadata_{std::make_shared<meta::Metadata>()},
      metadataServer_{rpc::serveMetadata(metadata_, "127.0.0.1", 0)},
      metadataEndpoint_{"127.0.0.1", metadataServer_->port()} {
  auto start = [&](Tier tier, const std::string& dir) {
    servers_.push_back(folly::coro::blockingWait(
        server::StorageServer::co_start(server::StorageServerOptions{
            .metadata = metadataEndpoint_,
            .bindHost = "127.0.0.1",
            .host = "127.0.0.1",
            .tier = tier,
            .regions = options.regionsPerServer,
            .regionSize = options.regionSize,
            .dir = dir})));
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
  metadataServer_.reset();
}

} // namespace kiln::local
