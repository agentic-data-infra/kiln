// kiln_meta: Kiln's metadata server.
//
//   kiln_meta [--port=P] [--bind_host=name]
//
// Storage servers register their regions here, and clients ask it for blocks
// and where chunks are. Its state is in memory. Stops on SIGINT or SIGTERM.

#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "bin/Signals.h"
#include "kiln/meta/Metadata.h"
#include "kiln/rpc/Rpc.h"

DEFINE_int32(port, 7470, "Port to serve on.");
DEFINE_string(bind_host, "", "Interface to serve on; empty for all.");

int main(int argc, char** argv) {
  FLAGS_logtostderr = true;
  folly::Init init(&argc, &argv);
  const auto signals = kiln::blockStopSignals();

  auto metadata = std::make_shared<kiln::meta::Metadata>();
  auto server = kiln::rpc::serveMetadata(
      metadata, FLAGS_bind_host, static_cast<uint16_t>(FLAGS_port));
  LOG(INFO) << "Metadata server on port " << server->port();

  const int signal = kiln::waitForStopSignal(signals);
  LOG(INFO) << "Stopping on signal " << signal;
  return 0;
}
