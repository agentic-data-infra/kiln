// kiln_server: a Kiln storage server.
//
//   kiln_server --metadata=URLS [--tier=dram|localfs] [--regions=N]
//               [--region_size_mb=M] [--budget_mb=B] [--dir=D] [--port=P]
//               [--host=name]
//               [--bind_host=name] [--rdma [--rdma_host=addr]]
//
// Creates its regions (files in /dev/shm for DRAM, preallocated files for
// the local FS), registers them with the metadata server, retrying until it
// is up and again every few seconds, and serves them. Stops on SIGINT or
// SIGTERM. A DRAM server's data is gone then; a local-FS server with --dir
// keeps its region files there and reattaches them, with their data, when it
// starts again. With --rdma (in a -c kiln.rdma=true build) DRAM regions are
// also served over RDMA.

#include <folly/coro/BlockingWait.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "bin/Signals.h"
#include "kiln/common/Error.h"
#include "kiln/server/StorageServer.h"

DEFINE_string(metadata, "", "The metadata server's URLs (kiln_meta prints them), or host:port; registers over TCP.");
DEFINE_string(tier, "dram", "dram or localfs.");
DEFINE_int32(regions, 1, "Regions to create.");
DEFINE_int64(region_size_mb, 1024, "Size of each region in MiB.");
DEFINE_int64(budget_mb, 0, "MiB of the regions to use at most; 0 for all.");
DEFINE_string(
    dir,
    "",
    "DRAM: where region files go, default /dev/shm. Local FS: this server's "
    "own directory, kept across restarts; default a new one in $TMPDIR.");
DEFINE_int32(port, 0, "Port to serve on; 0 for any.");
DEFINE_string(host, "", "Host name clients reach this server by.");
DEFINE_string(bind_host, "", "Interface to serve on; empty for all.");
DEFINE_int32(connect_timeout_s, 60, "Seconds to keep trying the metadata server.");
DEFINE_bool(rdma, false, "Also serve DRAM regions with RDMA (a -c kiln.rdma=true build).");
DEFINE_string(rdma_host, "", "Interface to serve RDMA on; empty for the first with an RDMA device.");

namespace {

// The metadata server's TCP endpoint, which storage servers register at.
kiln::Endpoint metadataTcp() {
  const auto address = kiln::MetadataAddress::parse(FLAGS_metadata);
  if (!address.tcp) {
    throw kiln::Error(
        kiln::ErrorCode::kInvalidArgument, "--metadata needs a tcp:// URL");
  }
  return *address.tcp;
}

} // namespace

int main(int argc, char** argv) {
  FLAGS_logtostderr = true;
  folly::Init init(&argc, &argv);
  if (FLAGS_metadata.empty()) {
    LOG(ERROR) << "--metadata is required";
    return 2;
  }
  const auto signals = kiln::blockStopSignals();

  std::unique_ptr<kiln::server::StorageServer> server;
  try {
    server = folly::coro::blockingWait(
        kiln::server::StorageServer::co_start(kiln::server::StorageServerOptions{
            .metadata = metadataTcp(),
            .bindHost = FLAGS_bind_host,
            .host = FLAGS_host,
            .port = static_cast<uint16_t>(FLAGS_port),
            .tier = kiln::parseTier(FLAGS_tier),
            .regions = static_cast<uint32_t>(FLAGS_regions),
            .regionSize = static_cast<uint64_t>(FLAGS_region_size_mb) << 20,
            .budget = static_cast<uint64_t>(FLAGS_budget_mb) << 20,
            .dir = FLAGS_dir,
            .connectTimeout = std::chrono::seconds(FLAGS_connect_timeout_s),
            .rdma = FLAGS_rdma,
            .rdmaHost = FLAGS_rdma_host}));
  } catch (const kiln::Error& e) {
    LOG(ERROR) << e.what();
    return 1;
  }

  const int signal = kiln::waitForStopSignal(signals);
  LOG(INFO) << "Stopping on signal " << signal;
  return 0;
}
