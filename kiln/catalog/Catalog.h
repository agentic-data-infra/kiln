// The catalog: memos of results that agents can find again instead of
// recomputing them. A client library on Kiln's public client API: manifests
// are chunks, and the pointers to them are refs.

#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include <folly/Range.h>
#include <folly/coro/Task.h>

#include "kiln/common/ChunkId.h"
#include "kiln/common/Types.h"

namespace kiln::client {
class Client;
}

namespace kiln::catalog {

// What a manifest entry's chunk holds.
enum class EntryKind : uint32_t {
  // Data.
  kChunk = 0,
  // Another manifest, e.g. a dataset's sub-directory.
  kManifest = 1,
};

// Bytes [offset, offset + length) of a chunk; to its end without a length.
// Datasets name their entries (file names) and give their lengths; memos
// need neither.
struct ChunkRange {
  ChunkId id;
  uint64_t offset{0};
  std::optional<uint64_t> length;
  std::string name;
  EntryKind kind{EntryKind::kChunk};
  std::optional<uint64_t> rows;

  bool operator==(const ChunkRange&) const = default;
};

// A result: the chunks that hold it and what the caller wants to know about
// it. Kiln does not interpret the schema or the properties.
struct Manifest {
  std::vector<ChunkRange> chunks;
  std::string schema;
  uint64_t rows{0};
  uint64_t bytes{0};
  std::map<std::string, std::string> properties;
  // A bucket that holds the listed chunks and goes with the manifest's
  // dataset (deleteDataset deletes it); kDefaultBucket for none.
  BucketId bucket{kDefaultBucket};

  bool operator==(const Manifest&) const = default;
};

// A version of a dataset: its manifest and the chunk that holds it.
struct Dataset {
  std::string name;
  // The ref's version, for publishDataset's compare and swap.
  uint64_t version{0};
  ChunkId manifestId;
  Manifest manifest;

  bool operator==(const Dataset&) const = default;
};

struct CatalogOptions {
  // Where manifests go. Written through to the local FS by default, so with
  // a journal they outlive restarts.
  Placement placement{.tier = Tier::kDram, .writeThrough = Tier::kLocalFs};
  // The names of memo refs start with this.
  std::string refPrefix{"memo/"};
  // The names of dataset refs start with this.
  std::string datasetRefPrefix{"data/"};
};

// Memos: keys (any bytes, e.g. a plan's fingerprint) to manifests. A memo is
// the ref "<refPrefix><hash of the key>" pointing at the manifest's chunk,
// which also holds the key.
//
// Datasets: names to versions of named files. A dataset is the ref
// "<datasetRefPrefix><name>" pointing at a manifest whose entries are its
// files; a new version is a new manifest and a compare and swap of the ref.
//
// Thread safe if the client is; calls fail with kiln::Error.
class Catalog {
 public:
  // 'client' must outlive the catalog.
  explicit Catalog(client::Client& client, CatalogOptions options = {});

  // The key's manifest if the manifest and all chunks it lists can be read
  // now; nullopt otherwise, and then the caller recomputes the result. A
  // memo whose chunks are gone (e.g. evicted, or ephemeral and lost in a
  // restart) is removed, so that it can be put again; one whose chunks are
  // only unavailable for now (their storage servers have not registered
  // again) is kept.
  folly::coro::Task<std::optional<Manifest>> co_getMemo(std::string key);

  // Writes the manifest and makes it the key's memo, unless the key has
  // one; returns whether it did.
  folly::coro::Task<bool> co_putMemo(std::string key, Manifest manifest);

  // Returns whether the key had a memo.
  folly::coro::Task<bool> co_removeMemo(std::string key);

  // The memo ref's name for a key.
  std::string refName(const std::string& key) const;

  // The dataset's current version, or nullopt if there is none. Does not
  // check that its chunks can be read.
  folly::coro::Task<std::optional<Dataset>> co_getDataset(std::string name);

  // Writes the manifest and points the dataset at it if the dataset is at
  // 'expectedVersion', or, with nullopt, if there is no such dataset.
  // Returns the new version, or nullopt if the version did not match (and
  // then the manifest's chunk is removed again). The previous version's
  // manifest and chunks stay. Names are one path segment: not empty, no
  // '/', at most kMaxRefNameSize with the prefix.
  folly::coro::Task<std::optional<uint64_t>> co_publishDataset(
      std::string name,
      Manifest manifest,
      std::optional<uint64_t> expectedVersion = std::nullopt);

  // The names of the datasets that start with 'prefix', in order.
  folly::coro::Task<std::vector<std::string>> co_listDatasets(
      std::string prefix = {});

  // Removes the dataset, then its manifest's chunk and bucket. Returns
  // whether there was one.
  folly::coro::Task<bool> co_deleteDataset(std::string name);

  // The dataset ref's name; throws Error(kInvalidArgument) for a bad name.
  std::string datasetRefName(const std::string& name) const;

 private:
  // Writes the encoded manifest as a chunk in kDefaultBucket.
  folly::coro::Task<ChunkId> co_writeManifest(
      std::string key,
      Manifest manifest);

  client::Client& client_;
  const CatalogOptions options_;
};

// A manifest and its key (a memo's key, a dataset's name) as bytes,
// versioned, and back. decodeManifest reads format versions 1 and 2 and
// throws Error(kInvalidArgument) for bytes it cannot read.
std::string encodeManifest(const std::string& key, const Manifest& manifest);
std::pair<std::string, Manifest> decodeManifest(folly::ByteRange data);

} // namespace kiln::catalog
