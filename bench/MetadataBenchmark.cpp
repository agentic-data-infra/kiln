// Latency of metadata calls over TCP (fbthrift) and over RDMA (rlib's RPC),
// one call at a time, against a metadata server's state in this process.
// Built only with -c kiln.rdma=true.
//
//   metadata_benchmark [--calls=N] [--blocks=B]
//
// TCP goes through the loopback device; RDMA through the first RDMA device,
// e.g. Soft-RoCE's rxe0, on its interface's address.

#include <algorithm>
#include <chrono>
#include <functional>
#include <vector>

#include <fmt/format.h>
#include <folly/coro/BlockingWait.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include "kiln/common/Error.h"
#include "kiln/meta/Metadata.h"
#include "kiln/rdma/RdmaMetadata.h"
#include "kiln/rpc/Rpc.h"

DEFINE_int32(calls, 10000, "Calls of each kind.");
DEFINE_int32(blocks, 16, "Blocks of the chunk that is looked up and committed.");

namespace {

using namespace kiln;
using Clock = std::chrono::steady_clock;
using folly::coro::blockingWait;

// The microseconds 'work' takes.
double timed(const std::function<void()>& work) {
  const auto start = Clock::now();
  work();
  return std::chrono::duration<double, std::micro>(Clock::now() - start)
      .count();
}

// Runs 'call', which returns the microseconds of the call it times, --calls
// times after a warm up, and prints the median and 99th percentile and the
// calls per second.
void measure(const std::string& name, const std::function<double()>& call) {
  for (int i = 0; i < 100; ++i) {
    call();
  }
  std::vector<double> micros;
  double total = 0;
  for (int i = 0; i < FLAGS_calls; ++i) {
    micros.push_back(call());
    total += micros.back();
  }
  std::sort(micros.begin(), micros.end());
  fmt::print(
      "{:<12} p50 {:6.1f} us  p99 {:6.1f} us  {:7.0f} calls/s\n",
      name,
      micros[micros.size() / 2],
      micros[micros.size() * 99 / 100],
      micros.size() / (total / 1e6));
}

void run(const std::string& transport, rpc::MetadataRpc& metadata) {
  // A chunk to look up.
  const auto count = static_cast<uint32_t>(FLAGS_blocks);
  auto blocks = blockingWait(metadata.co_allocate(Tier::kDram, count));
  ChunkInfo chunk{
      .id = ChunkId{.high = 1, .low = 1},
      .size = blocks.size() * kBlockSize,
      .replicas = {Replica{.tier = Tier::kDram, .blocks = blocks}}};
  blockingWait(metadata.co_commit(chunk));
  blockingWait(metadata.co_casRef("head", std::nullopt, "value"));

  measure(transport + " lookup", [&] {
    return timed([&] { blockingWait(metadata.co_lookup(chunk.id)); });
  });
  measure(transport + " getRef", [&] {
    return timed([&] { blockingWait(metadata.co_getRef("head")); });
  });
  uint64_t next = 2;
  measure(transport + " commit", [&] {
    // A new chunk each time; only the commit is timed.
    ChunkInfo fresh{
        .id = ChunkId{.high = next++, .low = 2},
        .size = count * kBlockSize,
        .replicas = {Replica{
            .tier = Tier::kDram,
            .blocks = blockingWait(metadata.co_allocate(Tier::kDram, count))}}};
    const auto micros =
        timed([&] { blockingWait(metadata.co_commit(fresh)); });
    blockingWait(metadata.co_remove(fresh.id));
    return micros;
  });
  blockingWait(metadata.co_remove(chunk.id));
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  auto metadata = std::make_shared<meta::Metadata>(meta::MetadataOptions{
      .gracePeriod = std::chrono::milliseconds(0),
      .evictBelow = 0,
      .evictUntil = 0});
  // A region to allocate from; nothing reads or writes it.
  blockingWait(metadata->co_registerServer(
      {RegionInfo{
          .server = "storage:1",
          .tier = Tier::kDram,
          .length = 64 * static_cast<uint64_t>(FLAGS_blocks) * kBlockSize}},
      0));

  const auto tcpServer = rpc::serveMetadata(metadata, "127.0.0.1", 0);
  std::unique_ptr<rdma::RdmaMetadataServer> rdmaServer;
  try {
    rdmaServer = rdma::RdmaMetadataServer::start(metadata, "", 0);
  } catch (const Error& e) {
    fmt::print("No RDMA: {}\n", e.what());
  }

  fmt::print(
      "{} calls of each kind, one at a time; chunks of {} blocks\n",
      FLAGS_calls,
      FLAGS_blocks);
  {
    auto tcp = rpc::connectMetadata(Endpoint{"127.0.0.1", tcpServer->port()});
    run("tcp", *tcp);
  }
  if (rdmaServer) {
    auto rdma = rdma::connectRdmaMetadata(rdmaServer->endpoint());
    run("rdma", *rdma);
  }
  return 0;
}
