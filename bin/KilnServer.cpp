// kiln_server: a Kiln storage server.
//
//   kiln_server --metadata=host:port [--tier=dram|localfs] [--regions=N]
//               [--region_size_mb=M] [--budget_mb=B] [--dir=D] [--port=P]
//               [--host=name]
//               [--bind_host=name]
//
// Creates its regions (files in /dev/shm for DRAM, preallocated files under
// --dir for the local FS), registers them with the metadata server, retrying
// until it is up, and serves them. Its data is gone when it stops, on SIGINT
// or SIGTERM.

#include <folly/coro/BlockingWait.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "bin/Signals.h"
#include "kiln/common/Error.h"
#include "kiln/server/StorageServer.h"

DEFINE_string(metadata, "", "The metadata server, host:port.");
DEFINE_string(tier, "dram", "dram or localfs.");
DEFINE_int32(regions, 1, "Regions to create.");
DEFINE_int64(region_size_mb, 1024, "Size of each region in MiB.");
DEFINE_int64(budget_mb, 0, "MiB of the regions to use at most; 0 for all.");
DEFINE_string(dir, "", "Where region files go; default /dev/shm or $TMPDIR.");
DEFINE_int32(port, 0, "Port to serve on; 0 for any.");
DEFINE_string(host, "", "Host name clients reach this server by.");
DEFINE_string(bind_host, "", "Interface to serve on; empty for all.");
DEFINE_int32(connect_timeout_s, 60, "Seconds to keep trying the metadata server.");

int main(int argc, char** argv) {
  FLAGS_logtostderr = true;
  folly::Init init(&argc, &argv);
  if (FLAGS_metadata.empty()) {
    LOG(ERROR) << "--metadata=host:port is required";
    return 2;
  }
  const auto signals = kiln::blockStopSignals();

  std::unique_ptr<kiln::server::StorageServer> server;
  try {
    server = folly::coro::blockingWait(
        kiln::server::StorageServer::co_start(kiln::server::StorageServerOptions{
            .metadata = kiln::Endpoint::parse(FLAGS_metadata),
            .bindHost = FLAGS_bind_host,
            .host = FLAGS_host,
            .port = static_cast<uint16_t>(FLAGS_port),
            .tier = kiln::parseTier(FLAGS_tier),
            .regions = static_cast<uint32_t>(FLAGS_regions),
            .regionSize = static_cast<uint64_t>(FLAGS_region_size_mb) << 20,
            .budget = static_cast<uint64_t>(FLAGS_budget_mb) << 20,
            .dir = FLAGS_dir,
            .connectTimeout = std::chrono::seconds(FLAGS_connect_timeout_s)}));
  } catch (const kiln::Error& e) {
    LOG(ERROR) << e.what();
    return 1;
  }

  const int signal = kiln::waitForStopSignal(signals);
  LOG(INFO) << "Stopping on signal " << signal;
  return 0;
}
