// kiln: Kiln's command line client.
//
//   kiln [--metadata=URLS] [--tier=dram|localfs]
//        [--write_through=localfs|objectstore] [--object_store=DIR]
//        put FILE
//   kiln [--metadata=URLS] get ID [FILE]
//   kiln [--metadata=URLS] stat ID
//   kiln [--metadata=URLS] rm ID
//   kiln [--metadata=URLS] ref get NAME
//   kiln [--metadata=URLS] ref set NAME VALUE [VERSION]
//   kiln [--metadata=URLS] ref ls [PREFIX]
//   kiln [--metadata=URLS] ref rm NAME [VERSION]
//   kiln [--metadata=URLS] [--tier=...] [--write_through=...]
//        data put PATH DATASET
//   kiln [--metadata=URLS] data ls [DATASET]
//   kiln [--metadata=URLS] data get DATASET[/FILE] [OUTPUT]
//   kiln [--metadata=URLS] data rm DATASET
//
// put stores FILE (- for stdin) as one chunk and prints its id; get writes
// the chunk to FILE or stdout. ref set creates the ref, or with VERSION
// updates it if it is at that version, and prints the new version.
//
// data works on the catalog's datasets: named versions of named files.
// data put creates DATASET from the file PATH, or from the files in the
// directory PATH, as chunks in a new bucket; it fails if DATASET exists.
// data ls lists the datasets, or DATASET's files; data get writes DATASET's
// FILE to OUTPUT or stdout, or all its files into the directory OUTPUT;
// data rm deletes DATASET with its files.
//
// --metadata is the metadata server's URLs, as kiln_meta prints them, e.g.
// tcp://host:7470,rdma://10.0.0.1:7471 (MetadataAddress); it defaults to
// $KILN_METADATA, else localhost:7470. Metadata calls go over RDMA if there
// is an rdma:// URL and RDMA is usable here, and data over RDMA for regions
// that offer it; --meta_force_tcp and --data_force_tcp make either use TCP.

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <filesystem>

#include <fmt/format.h>
#include <folly/Conv.h>
#include <folly/coro/BlockingWait.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include "kiln/catalog/Catalog.h"
#include "kiln/client/Client.h"
#include "kiln/common/Error.h"
#include "kiln/objectstore/ObjectStore.h"

DEFINE_string(
    metadata,
    "",
    "The metadata server's URLs, e.g. tcp://host:7470,rdma://10.0.0.1:7471.");
DEFINE_string(tier, "dram", "Where put and data put store chunks and manifests: dram or localfs.");
DEFINE_string(
    write_through,
    "",
    "Where put and data put also store them: localfs or objectstore; none "
    "if empty.");
DEFINE_string(
    object_store,
    "",
    "Directory of the local object store, to write through to and read from.");
DEFINE_bool(meta_force_tcp, false, "Make all metadata calls over TCP.");
DEFINE_bool(
    data_force_tcp,
    false,
    "Move all chunk data over TCP: no RDMA, no shared memory.");
DEFINE_bool(
    no_shm,
    false,
    "Do not map regions on this host; reach them with RDMA or TCP.");

namespace kiln {
namespace {

constexpr size_t kPieceSize = 8 << 20;

Placement placementFromFlags() {
  Placement placement{.tier = parseTier(FLAGS_tier)};
  if (!FLAGS_write_through.empty()) {
    placement.writeThrough = parseTier(FLAGS_write_through);
  }
  return placement;
}

// Stores the file at 'path' (- for stdin) as one chunk in 'bucket'; returns
// its id and size.
folly::coro::Task<std::pair<ChunkId, uint64_t>> co_putFile(
    client::Client& client,
    std::string path,
    BucketId bucket) {
  FILE* file = path == "-" ? stdin : std::fopen(path.c_str(), "rb");
  if (file == nullptr) {
    throw Error(ErrorCode::kInvalidArgument, fmt::format("Cannot open {}", path));
  }
  auto writer = co_await client.co_create(bucket, placementFromFlags());
  auto buffer = client.allocateBuffer(kPieceSize);
  uint64_t size = 0;
  for (;;) {
    const auto n = std::fread(buffer.data(), 1, buffer.capacity(), file);
    if (n == 0) {
      break;
    }
    size += n;
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
  const auto id = co_await writer->co_commit();
  co_return std::pair{id, size};
}

folly::coro::Task<void> co_put(client::Client& client, std::string path) {
  const auto [id, size] = co_await co_putFile(client, path, kDefaultBucket);
  fmt::print("{}\n", id.toString());
}

// Writes 'length' bytes of the chunk from 'offset' (to its end without a
// length) to the file at 'path', or stdout if empty.
folly::coro::Task<void> co_get(
    client::Client& client,
    ChunkId id,
    std::string path,
    uint64_t offset = 0,
    std::optional<uint64_t> length = std::nullopt) {
  FILE* file = path.empty() ? stdout : std::fopen(path.c_str(), "wb");
  if (file == nullptr) {
    throw Error(ErrorCode::kInvalidArgument, fmt::format("Cannot create {}", path));
  }
  uint64_t end;
  if (length) {
    end = offset + *length;
  } else {
    end = (co_await client.co_stat(id)).size;
  }
  auto buffer = client.allocateBuffer(kPieceSize);
  while (offset < end) {
    const auto piece = std::min<uint64_t>(kPieceSize, end - offset);
    buffer = co_await client.co_read(id, offset, piece, std::move(buffer));
    if (buffer.size() == 0) {
      throw Error(ErrorCode::kInvalidArgument, "The chunk is cut short");
    }
    if (std::fwrite(buffer.data(), 1, buffer.size(), file) != buffer.size()) {
      throw Error(ErrorCode::kInvalidArgument, "Cannot write the output");
    }
    offset += buffer.size();
  }
  if (file != stdout) {
    std::fclose(file);
  }
}

folly::coro::Task<void> co_stat(client::Client& client, ChunkId id) {
  const auto chunk = co_await client.co_stat(id);
  fmt::print("id: {}\nsize: {}\n", chunk.id.toString(), chunk.size);
  for (const auto& replica : chunk.replicas) {
    fmt::print(
        "{}: {} blocks\n", toString(replica.tier), replica.blocks.size());
    for (const auto& block : replica.blocks) {
      fmt::print(
          "  region {} offset {} length {}\n",
          block.region,
          block.offset,
          block.length);
    }
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

// The files to store for 'path': itself, or a directory's regular files in
// name order.
std::vector<std::filesystem::path> filesOf(const std::string& path) {
  if (!std::filesystem::is_directory(path)) {
    return {path};
  }
  std::vector<std::filesystem::path> files;
  for (const auto& entry : std::filesystem::directory_iterator(path)) {
    if (entry.is_regular_file()) {
      files.push_back(entry.path());
    }
  }
  std::sort(files.begin(), files.end());
  return files;
}

folly::coro::Task<int> co_dataPut(
    client::Client& client,
    catalog::Catalog& catalog,
    std::string path,
    std::string name) {
  // Fails early for a bad name, before any chunk is written.
  catalog.datasetRefName(name);
  const auto bucket = co_await client.co_createBucket();
  catalog::Manifest manifest{.bucket = bucket};
  std::exception_ptr error;
  try {
    for (const auto& file : filesOf(path)) {
      const auto [id, size] = co_await co_putFile(client, file.string(), bucket);
      manifest.chunks.push_back(
          {.id = id, .length = size, .name = file.filename().string()});
      manifest.bytes += size;
    }
  } catch (...) {
    error = std::current_exception();
  }
  if (error) {
    co_await client.co_deleteBucket(bucket);
    std::rethrow_exception(error);
  }
  const auto files = manifest.chunks.size();
  const auto bytes = manifest.bytes;
  const auto version =
      co_await catalog.co_publishDataset(name, std::move(manifest));
  if (!version) {
    co_await client.co_deleteBucket(bucket);
    fmt::print(stderr, "Dataset {} exists\n", name);
    co_return 1;
  }
  fmt::print("{}\t{}\t{} files\t{} bytes\n", name, *version, files, bytes);
  co_return 0;
}

folly::coro::Task<int> co_dataLs(
    catalog::Catalog& catalog,
    std::optional<std::string> name) {
  if (!name) {
    for (const auto& dataset : co_await catalog.co_listDatasets()) {
      const auto current = co_await catalog.co_getDataset(dataset);
      if (current) {
        fmt::print(
            "{}\t{}\t{} files\t{} bytes\n",
            current->name,
            current->version,
            current->manifest.chunks.size(),
            current->manifest.bytes);
      }
    }
    co_return 0;
  }
  const auto dataset = co_await catalog.co_getDataset(*name);
  if (!dataset) {
    fmt::print(stderr, "No dataset {}\n", *name);
    co_return 1;
  }
  for (const auto& entry : dataset->manifest.chunks) {
    if (entry.kind == catalog::EntryKind::kManifest) {
      fmt::print("{}/\t-\t{}\n", entry.name, entry.id.toString());
    } else {
      fmt::print(
          "{}\t{}\t{}\n",
          entry.name,
          entry.length.value_or(0),
          entry.id.toString());
    }
  }
  co_return 0;
}

folly::coro::Task<int> co_dataGet(
    client::Client& client,
    catalog::Catalog& catalog,
    std::string path,
    std::string output) {
  const auto slash = path.find('/');
  const auto name = path.substr(0, slash);
  const auto dataset = co_await catalog.co_getDataset(name);
  if (!dataset) {
    fmt::print(stderr, "No dataset {}\n", name);
    co_return 1;
  }
  if (slash == std::string::npos) {
    if (output.empty()) {
      fmt::print(stderr, "data get DATASET needs an output directory\n");
      co_return 2;
    }
    std::filesystem::create_directories(output);
  }
  bool found = false;
  for (const auto& entry : dataset->manifest.chunks) {
    if (slash != std::string::npos && entry.name != path.substr(slash + 1)) {
      continue;
    }
    if (entry.kind == catalog::EntryKind::kManifest) {
      throw Error(
          ErrorCode::kInvalidArgument,
          fmt::format("{} is a directory, which data get cannot read yet", entry.name));
    }
    found = true;
    const auto target = slash == std::string::npos
        ? (std::filesystem::path(output) / entry.name).string()
        : output;
    co_await co_get(client, entry.id, target, entry.offset, entry.length);
  }
  if (slash != std::string::npos && !found) {
    fmt::print(stderr, "No file {}\n", path);
    co_return 1;
  }
  co_return 0;
}

// "kiln data ..."; returns the exit code.
folly::coro::Task<int> co_data(
    client::Client& client,
    const std::vector<std::string>& args) {
  // Manifests go where the files go: the catalog's default writes them
  // through to the local FS, which a cluster without a local-FS server lacks.
  catalog::Catalog catalog(
      client, catalog::CatalogOptions{.placement = placementFromFlags()});
  const std::string command = args.size() > 1 ? args[1] : "";
  if (command == "put" && args.size() == 4) {
    co_return co_await co_dataPut(client, catalog, args[2], args[3]);
  } else if (command == "ls" && (args.size() == 2 || args.size() == 3)) {
    std::optional<std::string> name;
    if (args.size() == 3) {
      name = args[2];
    }
    co_return co_await co_dataLs(catalog, name);
  } else if (command == "get" && (args.size() == 3 || args.size() == 4)) {
    // Named: GCC 12 destroys a ?: temporary in a co_await's arguments twice.
    const std::string output = args.size() == 4 ? args[3] : "";
    co_return co_await co_dataGet(client, catalog, args[2], output);
  } else if (command == "rm" && args.size() == 3) {
    if (!co_await catalog.co_deleteDataset(args[2])) {
      fmt::print(stderr, "No dataset {}\n", args[2]);
      co_return 1;
    }
    co_return 0;
  }
  fmt::print(
      stderr,
      "Usage: kiln data put PATH DATASET | ls [DATASET] | "
      "get DATASET[/FILE] [OUTPUT] | rm DATASET\n");
  co_return 2;
}

folly::coro::Task<int> co_main(const std::vector<std::string>& args) {
  std::string metadata = FLAGS_metadata;
  if (metadata.empty()) {
    const char* env = std::getenv("KILN_METADATA");
    metadata = env != nullptr ? env : "localhost:7470";
  }
  client::ClientOptions options{
      .metadata = MetadataAddress::parse(metadata),
      .metaForceTcp = FLAGS_meta_force_tcp,
      .dataForceTcp = FLAGS_data_force_tcp,
      .useShm = !FLAGS_no_shm};
  if (!FLAGS_object_store.empty()) {
    options.objectStore = objectstore::makeLocalObjectStore(FLAGS_object_store);
  }
  client::Client client(std::move(options));

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
  } else if (command == "data") {
    co_return co_await co_data(client, args);
  } else {
    fmt::print(stderr, "Usage: kiln put FILE | get ID [FILE] | stat ID | rm ID | ref ... | data ...\n");
    co_return 2;
  }
  co_return 0;
}

} // namespace
} // namespace kiln

int main(int argc, char** argv) {
  gflags::SetUsageMessage("kiln put FILE | get ID [FILE] | stat ID | rm ID | ref ... | data ...");
  folly::Init init(&argc, &argv);
  if (argc < 2) {
    fmt::print(stderr, "Usage: kiln put FILE | get ID [FILE] | stat ID | rm ID | ref ... | data ...\n");
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
