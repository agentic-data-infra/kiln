// kiln: Kiln's command line client.
//
//   kiln [--metadata=host:port] [--tier=dram|localfs] [--tcp_only] put FILE
//   kiln [--metadata=host:port] get ID [FILE]
//   kiln [--metadata=host:port] stat ID
//   kiln [--metadata=host:port] rm ID
//   kiln [--metadata=host:port] ref get NAME
//   kiln [--metadata=host:port] ref set NAME VALUE [VERSION]
//   kiln [--metadata=host:port] ref ls [PREFIX]
//   kiln [--metadata=host:port] ref rm NAME [VERSION]
//
// put stores FILE (- for stdin) as one chunk and prints its id; get writes
// the chunk to FILE or stdout. ref set creates the ref, or with VERSION
// updates it if it is at that version, and prints the new version. --metadata defaults to $KILN_METADATA, else
// localhost:7470.

#include <cstdio>
#include <cstdlib>

#include <fmt/format.h>
#include <folly/Conv.h>
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

// "kiln ref ..."; returns the exit code.
folly::coro::Task<int> co_ref(
    client::Client& client,
    const std::vector<std::string>& args) {
  const std::string command = args.size() > 1 ? args[1] : "";
  std::optional<uint64_t> version;
  auto parseVersion = [&](size_t i) {
    if (args.size() > i) {
      const auto parsed = folly::tryTo<uint64_t>(args[i]);
      if (!parsed) {
        throw Error(
            ErrorCode::kInvalidArgument,
            fmt::format("Bad version '{}'", args[i]));
      }
      version = *parsed;
    }
  };
  if (command == "get" && args.size() == 3) {
    auto ref = co_await client.co_getRef(args[2]);
    if (!ref) {
      fmt::print(stderr, "No ref {}\n", args[2]);
      co_return 1;
    }
    fmt::print("{}\t{}\n", ref->version, ref->value);
  } else if (command == "set" && (args.size() == 4 || args.size() == 5)) {
    parseVersion(4);
    auto ref = co_await client.co_casRef(args[2], version, args[3]);
    if (!ref) {
      fmt::print(stderr, "Ref {} is not at the expected version\n", args[2]);
      co_return 1;
    }
    fmt::print("{}\n", ref->version);
  } else if (command == "ls" && (args.size() == 2 || args.size() == 3)) {
    // Named: GCC 12 destroys a ?: temporary in a co_await's arguments twice.
    const std::string prefix = args.size() == 3 ? args[2] : "";
    for (const auto& ref : co_await client.co_listRefs(prefix)) {
      fmt::print("{}\t{}\t{}\n", ref.name, ref.version, ref.value);
    }
  } else if (command == "rm" && (args.size() == 3 || args.size() == 4)) {
    parseVersion(3);
    if (!co_await client.co_removeRef(args[2], version)) {
      fmt::print(stderr, "No ref {} at the expected version\n", args[2]);
      co_return 1;
    }
  } else {
    fmt::print(
        stderr,
        "Usage: kiln ref get NAME | set NAME VALUE [VERSION] | ls [PREFIX] | "
        "rm NAME [VERSION]\n");
    co_return 2;
  }
  co_return 0;
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
  } else if (command == "ref") {
    co_return co_await co_ref(client, args);
  } else {
    fmt::print(stderr, "Usage: kiln put FILE | get ID [FILE] | stat ID | rm ID | ref ...\n");
    co_return 2;
  }
  co_return 0;
}

} // namespace
} // namespace kiln

int main(int argc, char** argv) {
  gflags::SetUsageMessage("kiln put FILE | get ID [FILE] | stat ID | rm ID | ref ...");
  folly::Init init(&argc, &argv);
  if (argc < 2) {
    fmt::print(stderr, "Usage: kiln put FILE | get ID [FILE] | stat ID | rm ID | ref ...\n");
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
