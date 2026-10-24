// Stores a file in Kiln as one chunk, reads it back and checks it.
//
//   put_get_file [--metadata=host:port] [--tier=dram|localfs] FILE
//
// Without --metadata it runs against a cluster in this process.

#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>

#include <fmt/format.h>
#include <folly/coro/BlockingWait.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include "kiln/client/Client.h"
#include "kiln/local/LocalCluster.h"

DEFINE_string(metadata, "", "The metadata server; empty for an in-process cluster.");
DEFINE_string(tier, "dram", "dram or localfs.");

namespace {

using namespace kiln;
constexpr size_t kPieceSize = 4 << 20;

double secondsSince(std::chrono::steady_clock::time_point start) {
  return std::chrono::duration<double>(std::chrono::steady_clock::now() - start)
      .count();
}

folly::coro::Task<void> co_run(client::Client& client, const std::string& path) {
  std::ifstream file(path, std::ios::binary);
  if (!file) {
    throw std::runtime_error("Cannot open " + path);
  }

  // Write: append the file in pieces, reusing one buffer.
  auto start = std::chrono::steady_clock::now();
  auto writer =
      co_await client.co_create(Placement{.tier = parseTier(FLAGS_tier)});
  auto buffer = client.allocateBuffer(kPieceSize);
  while (file) {
    file.read(reinterpret_cast<char*>(buffer.data()), buffer.capacity());
    buffer.setSize(file.gcount());
    buffer = co_await writer->co_append(std::move(buffer));
  }
  const auto id = co_await writer->co_commit();
  const auto info = co_await client.co_stat(id);
  fmt::print(
      "put {} ({} bytes, {} blocks in {}) in {:.3f}s\n",
      id.toString(),
      info.size,
      info.replicas[0].blocks.size(),
      toString(info.placement.tier),
      secondsSince(start));

  // Read: the whole chunk into one buffer, then compare with the file.
  start = std::chrono::steady_clock::now();
  auto data = co_await client.co_read(
      id, 0, info.size, client.allocateBuffer(info.size));
  fmt::print("got {} bytes in {:.3f}s\n", data.size(), secondsSince(start));

  file.clear();
  file.seekg(0);
  std::string expected(info.size, '\0');
  file.read(expected.data(), expected.size());
  const bool same =
      std::memcmp(expected.data(), data.data(), data.size()) == 0;
  fmt::print("{}\n", same ? "contents match" : "CONTENTS DIFFER");
  co_await client.co_remove(id);
  if (!same) {
    throw std::runtime_error("Contents differ");
  }
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  if (argc != 2) {
    fmt::print(stderr, "Usage: put_get_file [--metadata=host:port] FILE\n");
    return 2;
  }
  std::unique_ptr<local::LocalCluster> cluster;
  auto metadata = FLAGS_metadata;
  if (metadata.empty()) {
    cluster = std::make_unique<local::LocalCluster>(local::LocalClusterOptions{
        .dramServers = 2, .localFsServers = 2, .regionSize = 256 << 20});
    metadata = cluster->metadataEndpoint().toString();
  }
  client::Client client(
      client::ClientOptions{.metadata = Endpoint::parse(metadata)});
  folly::coro::blockingWait(co_run(client, argv[1]));
  return 0;
}
