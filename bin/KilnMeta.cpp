// kiln_meta: Kiln's metadata server.
//
//   kiln_meta [--port=P] [--bind_host=name] [--grace_period_s=S]
//             [--journal=FILE] [--rdma [--rdma_host=addr] [--rdma_port=P]]
//
// Storage servers register their regions here, and clients ask it for blocks
// and where chunks are, and for refs. Its state is in memory; with a journal,
// refs survive restarts. With --rdma (in a -c kiln.rdma=true build) it also
// answers over RDMA. It logs its URLs, e.g. tcp://host:7470,rdma://addr:7471,
// for clients (kiln --metadata). Stops on SIGINT or SIGTERM.

#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "bin/Signals.h"
#include "kiln/meta/Metadata.h"
#include "kiln/rpc/Rpc.h"
#if KILN_RDMA
#include "kiln/common/Error.h"
#include "kiln/rdma/RdmaMetadata.h"
#endif

DEFINE_int32(port, 7470, "Port to serve on.");
DEFINE_string(bind_host, "", "Interface to serve on; empty for all.");
DEFINE_int32(
    grace_period_s,
    30,
    "Seconds before blocks of removed or evicted chunks are reused.");
DEFINE_string(
    journal,
    "",
    "File that keeps refs across restarts; without one they are lost.");
DEFINE_bool(rdma, false, "Also serve over RDMA (a -c kiln.rdma=true build).");
DEFINE_string(rdma_host, "", "Interface to serve RDMA on; empty for the first with an RDMA device.");
DEFINE_int32(rdma_port, 7471, "Port to serve RDMA on.");

int main(int argc, char** argv) {
  FLAGS_logtostderr = true;
  folly::Init init(&argc, &argv);
  const auto signals = kiln::blockStopSignals();

  auto metadata =
      std::make_shared<kiln::meta::Metadata>(kiln::meta::MetadataOptions{
          .gracePeriod = std::chrono::seconds(FLAGS_grace_period_s),
          .journal = FLAGS_journal});
  auto server = kiln::rpc::serveMetadata(
      metadata, FLAGS_bind_host, static_cast<uint16_t>(FLAGS_port));
  kiln::MetadataAddress address(kiln::Endpoint{
      .host = FLAGS_bind_host.empty() ? kiln::localHostName() : FLAGS_bind_host,
      .port = server->port()});
#if KILN_RDMA
  std::unique_ptr<kiln::rdma::RdmaMetadataServer> rdmaServer;
  if (FLAGS_rdma) {
    try {
      rdmaServer = kiln::rdma::RdmaMetadataServer::start(
          metadata, FLAGS_rdma_host, static_cast<uint16_t>(FLAGS_rdma_port));
    } catch (const kiln::Error& e) {
      LOG(ERROR) << e.what();
      return 1;
    }
    address.rdma = rdmaServer->endpoint();
  }
#else
  if (FLAGS_rdma) {
    LOG(ERROR) << "--rdma needs a build with -c kiln.rdma=true";
    return 1;
  }
#endif
  // For clients' ClientOptions::metadata (kiln --metadata).
  LOG(INFO) << "Metadata server at " << address.toString();

  const int signal = kiln::waitForStopSignal(signals);
  LOG(INFO) << "Stopping on signal " << signal;
  return 0;
}
