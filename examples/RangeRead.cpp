// Reads parts of a chunk without reading all of it: a chunk of fixed-size
// records, and lookups of single records and of a span that crosses blocks.
//
//   range_read [--metadata=host:port] [--records=N]
//
// Without --metadata it runs against a cluster in this process.

#include <fmt/format.h>
#include <folly/coro/BlockingWait.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include "kiln/client/Client.h"
#include "kiln/local/LocalCluster.h"

DEFINE_string(metadata, "", "The metadata server; empty for an in-process cluster.");
DEFINE_int64(records, 200'000, "Records in the chunk.");

namespace {

using namespace kiln;

// A record: 64 bytes holding its number in text.
constexpr size_t kRecordSize = 64;

void formatRecord(int64_t i, uint8_t* out) {
  std::memset(out, ' ', kRecordSize);
  auto text = fmt::format("record {:>12}", i);
  std::memcpy(out, text.data(), text.size());
  out[kRecordSize - 1] = '\n';
}

std::string text(const Buffer& buffer, size_t offset, size_t length) {
  return std::string(
      reinterpret_cast<const char*>(buffer.data()) + offset, length);
}

folly::coro::Task<void> co_run(client::Client& client) {
  const auto size = FLAGS_records * kRecordSize;
  auto writer = co_await client.co_create(kDefaultBucket);
  auto buffer = client.allocateBuffer(size);
  for (int64_t i = 0; i < FLAGS_records; ++i) {
    formatRecord(i, buffer.data() + i * kRecordSize);
  }
  buffer.setSize(size);
  co_await writer->co_append(std::move(buffer));
  const auto id = co_await writer->co_commit();
  const auto info = co_await client.co_stat(id);
  fmt::print(
      "chunk {}: {} records, {} bytes in {} blocks\n",
      id.toString(),
      FLAGS_records,
      info.size,
      info.replicas[0].blocks.size());

  // Single records, read into one small buffer.
  auto record = client.allocateBuffer(kRecordSize);
  for (int64_t i : {int64_t{0}, FLAGS_records / 2, FLAGS_records - 1}) {
    record = co_await client.co_read(
        id, i * kRecordSize, kRecordSize, std::move(record));
    fmt::print("record {}: {}", i, text(record, 0, kRecordSize));
  }

  // 100 records around the first block boundary.
  const auto first = kBlockSize / kRecordSize - 50;
  if (first + 100 <= static_cast<uint64_t>(FLAGS_records)) {
    auto span = co_await client.co_read(
        id,
        first * kRecordSize,
        100 * kRecordSize,
        client.allocateBuffer(100 * kRecordSize));
    fmt::print(
        "records {}..{} across the block boundary:\n  {}  ...\n  {}",
        first,
        first + 99,
        text(span, 0, kRecordSize),
        text(span, 99 * kRecordSize, kRecordSize));
  }

  // Past the end, a read returns what there is.
  auto tail = co_await client.co_read(
      id, size - 10, 1000, client.allocateBuffer(1000));
  fmt::print("read of 1000 bytes 10 before the end returned {}\n", tail.size());
  co_await client.co_remove(id);
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  std::unique_ptr<local::LocalCluster> cluster;
  auto metadata = FLAGS_metadata;
  if (metadata.empty()) {
    cluster = std::make_unique<local::LocalCluster>();
    metadata = cluster->metadataEndpoint().toString();
  }
  client::Client client(
      client::ClientOptions{.metadata = Endpoint::parse(metadata)});
  folly::coro::blockingWait(co_run(client));
  return 0;
}
