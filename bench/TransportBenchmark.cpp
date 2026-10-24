// Write and read throughput of one chunk over TCP and over shared memory, in
// each tier, next to reading and writing a plain local file.
//
//   transport_benchmark [--size_mb=N] [--rounds=R] [--piece_mb=P]
//
// Runs against a cluster in this process (on 127.0.0.1), so TCP goes through
// the loopback device.

#include <fcntl.h>
#include <unistd.h>

#include <chrono>
#include <filesystem>

#include <fmt/format.h>
#include <folly/coro/BlockingWait.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include "kiln/client/Client.h"
#include "kiln/local/LocalCluster.h"

DEFINE_int64(size_mb, 256, "Chunk size in MiB.");
DEFINE_int32(rounds, 5, "Times each chunk is read.");
DEFINE_int64(piece_mb, 8, "Bytes per append, in MiB.");

namespace {

using namespace kiln;
using Clock = std::chrono::steady_clock;

double gbPerSecond(uint64_t bytes, Clock::time_point start) {
  const auto seconds =
      std::chrono::duration<double>(Clock::now() - start).count();
  return bytes / seconds / 1e9;
}

folly::coro::Task<void>
co_measure(client::Client& client, Tier tier, const std::string& name) {
  const uint64_t size = FLAGS_size_mb << 20;
  const uint64_t piece = FLAGS_piece_mb << 20;
  auto buffer = client.allocateBuffer(piece);
  for (uint64_t i = 0; i < piece; ++i) {
    buffer.data()[i] = static_cast<uint8_t>(i * 7 + 1);
  }
  buffer.setSize(piece);

  auto start = Clock::now();
  auto writer = co_await client.co_create(Placement{.tier = tier});
  for (uint64_t done = 0; done < size; done += piece) {
    // Vary one byte per piece so the chunk is not all repeats.
    buffer.data()[0] = static_cast<uint8_t>(done / piece);
    buffer = co_await writer->co_append(std::move(buffer));
  }
  const auto id = co_await writer->co_commit();
  const auto write = gbPerSecond(size, start);

  auto data = client.allocateBuffer(size);
  start = Clock::now();
  for (int i = 0; i < FLAGS_rounds; ++i) {
    data = co_await client.co_read(id, 0, size, std::move(data));
  }
  const auto read = gbPerSecond(size * FLAGS_rounds, start);
  fmt::print(
      "{:<8} {:<4} write {:6.2f} GB/s  read {:6.2f} GB/s\n",
      toString(tier),
      name,
      write,
      read);
  co_await client.co_remove(id);
}

// The same sizes against a file in $TMPDIR, through the page cache.
void measureLocalFile() {
  const uint64_t size = FLAGS_size_mb << 20;
  const uint64_t piece = FLAGS_piece_mb << 20;
  const auto path = std::filesystem::temp_directory_path() /
      fmt::format("kiln-bench-{}", ::getpid());
  std::vector<uint8_t> buffer(piece, 1);
  auto data = Buffer::allocate(size);

  auto start = Clock::now();
  const int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0600);
  for (uint64_t done = 0; done < size; done += piece) {
    if (::pwrite(fd, buffer.data(), piece, done) != static_cast<ssize_t>(piece)) {
      throw std::runtime_error("Cannot write " + path.string());
    }
  }
  const auto write = gbPerSecond(size, start);
  start = Clock::now();
  for (int i = 0; i < FLAGS_rounds; ++i) {
    for (uint64_t done = 0; done < size;) {
      auto n = ::pread(fd, data.data() + done, size - done, done);
      if (n <= 0) {
        throw std::runtime_error("Cannot read " + path.string());
      }
      done += n;
    }
  }
  const auto read = gbPerSecond(size * FLAGS_rounds, start);
  ::close(fd);
  std::filesystem::remove(path);
  fmt::print(
      "{:<13} write {:6.2f} GB/s  read {:6.2f} GB/s\n", "local file", write, read);
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  // Room for one chunk per tier and server; blocks spread over both.
  const uint64_t regionSize = ((FLAGS_size_mb + 1) / 2 + 1) << 20;
  // Each chunk is removed before the next is written; its blocks are
  // reusable at once.
  local::LocalCluster cluster(local::LocalClusterOptions{
      .dramServers = 2,
      .localFsServers = 2,
      .regionSize = regionSize,
      .metadata = meta::MetadataOptions{
          .gracePeriod = std::chrono::milliseconds(0)}});

  fmt::print(
      "{} MiB chunk, appended in {} MiB pieces, read {} times\n",
      FLAGS_size_mb,
      FLAGS_piece_mb,
      FLAGS_rounds);
  for (bool shm : {false, true}) {
    client::Client client(client::ClientOptions{
        .metadata = cluster.metadataEndpoint(), .useShm = shm});
    for (auto tier : {Tier::kDram, Tier::kLocalFs}) {
      folly::coro::blockingWait(co_measure(client, tier, shm ? "shm" : "tcp"));
    }
  }
  measureLocalFile();
  return 0;
}
