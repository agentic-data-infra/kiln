// kiln: Kiln's command line client.
//
//   kiln [--metadata=host:port] [--tier=dram|localfs] [--tcp_only] put FILE
//   kiln [--metadata=host:port] get ID [FILE]
//   kiln [--metadata=host:port] stat ID
//   kiln [--metadata=host:port] rm ID
//
// put stores FILE (- for stdin) as one chunk and prints its id; get writes
// the chunk to FILE or stdout. --metadata defaults to $KILN_METADATA, else
// localhost:7470.

#include <cstdio>
#include <cstdlib>

#include <fmt/format.h>
#include <folly/coro/BlockingWait.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include "kiln/client/Client.h"
#include "kiln/common/Error.h"

DEFINE_string(metadata, "", "The metadata server, host:port.");
DEFINE_string(tier, "dram", "Where put stores the chunk: dram or localfs.");
DEFINE_bool(tcp_only, false, "Use TCP even for storage servers on this host.");

namespace kiln {
namespace {

constexpr size_t kPieceSize = 8 << 20;

folly::coro::Task<void> co_put(client::Client& client, const std::string& path) {
  FILE* file = path == "-" ? stdin : std::fopen(path.c_str(), "rb");
  if (file == nullptr) {
    throw Error(ErrorCode::kInvalidArgument, fmt::format("Cannot open {}", path));
  }
  auto writer =
      co_await client.co_create(Placement{.tier = parseTier(FLAGS_tier)});
  auto buffer = client.allocateBuffer(kPieceSize);
  for (;;) {
    const auto n = std::fread(buffer.data(), 1, buffer.capacity(), file);
    if (n == 0) {
      break;
    }
    buffer.setSize(n);
    buffer = co_await writer->co_append(std::move(buffer));
  }
  const bool failed = std::ferror(file);
  if (file != stdin) {
    std::fclose(file);
  }
  if (failed) {
    co_await writer->co_abort();
    throw Error(ErrorCode::kInvalidArgument, fmt::format("Cannot read {}", path));
  }
  fmt::print("{}\n", (co_await writer->co_commit()).toString());
}

folly::coro::Task<void>
co_get(client::Client& client, ChunkId id, const std::string& path) {
  FILE* file = path.empty() ? stdout : std::fopen(path.c_str(), "wb");
  if (file == nullptr) {
    throw Error(ErrorCode::kInvalidArgument, fmt::format("Cannot create {}", path));
  }
  const auto size = (co_await client.co_stat(id)).size;
  auto buffer = client.allocateBuffer(kPieceSize);
  for (uint64_t offset = 0; offset < size; offset += buffer.size()) {
    buffer = co_await client.co_read(id, offset, kPieceSize, std::move(buffer));
    if (std::fwrite(buffer.data(), 1, buffer.size(), file) != buffer.size()) {
      throw Error(ErrorCode::kInvalidArgument, "Cannot write the output");
    }
  }
  if (file != stdout) {
    std::fclose(file);
  }
}

folly::coro::Task<void> co_stat(client::Client& client, ChunkId id) {
  const auto chunk = co_await client.co_stat(id);
  fmt::print(
      "id: {}\nsize: {}\ntier: {}\nblocks: {}\n",
      chunk.id.toString(),
      chunk.size,
      toString(chunk.placement.tier),
      chunk.blocks.size());
  for (const auto& block : chunk.blocks) {
    fmt::print(
        "  region {} offset {} length {}\n",
        block.region,
        block.offset,
        block.length);
  }
}

folly::coro::Task<int> co_main(const std::vector<std::string>& args) {
  std::string metadata = FLAGS_metadata;
  if (metadata.empty()) {
    const char* env = std::getenv("KILN_METADATA");
    metadata = env != nullptr ? env : "localhost:7470";
  }
  client::Client client(client::ClientOptions{
      .metadata = Endpoint::parse(metadata), .useShm = !FLAGS_tcp_only});

  const auto& command = args[0];
  if (command == "put" && args.size() == 2) {
    co_await co_put(client, args[1]);
  } else if (command == "get" && (args.size() == 2 || args.size() == 3)) {
    // Named: GCC 12 destroys a ?: temporary in a co_await's arguments twice.
    const std::string output = args.size() == 3 ? args[2] : "";
    co_await co_get(client, ChunkId::parse(args[1]), output);
  } else if (command == "stat" && args.size() == 2) {
    co_await co_stat(client, ChunkId::parse(args[1]));
  } else if (command == "rm" && args.size() == 2) {
    if (!co_await client.co_remove(ChunkId::parse(args[1]))) {
      fmt::print(stderr, "No chunk {}\n", args[1]);
      co_return 1;
    }
  } else {
    fmt::print(stderr, "Usage: kiln put FILE | get ID [FILE] | stat ID | rm ID\n");
    co_return 2;
  }
  co_return 0;
}

} // namespace
} // namespace kiln

int main(int argc, char** argv) {
  gflags::SetUsageMessage("kiln put FILE | get ID [FILE] | stat ID | rm ID");
  folly::Init init(&argc, &argv);
  if (argc < 2) {
    fmt::print(stderr, "Usage: kiln put FILE | get ID [FILE] | stat ID | rm ID\n");
    return 2;
  }
  const std::vector<std::string> args(argv + 1, argv + argc);
  try {
    return folly::coro::blockingWait(kiln::co_main(args));
  } catch (const kiln::Error& e) {
    fmt::print(stderr, "kiln: {} ({})\n", e.what(), kiln::toString(e.code()));
    return 1;
  }
}
