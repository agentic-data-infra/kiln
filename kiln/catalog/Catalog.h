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

// Bytes [offset, offset + length) of a chunk; to its end without a length.
struct ChunkRange {
  ChunkId id;
  uint64_t offset{0};
  std::optional<uint64_t> length;

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

  bool operator==(const Manifest&) const = default;
};

struct CatalogOptions {
  // Where manifests go. Written through to the local FS by default, so with
  // a journal they outlive restarts.
  Placement placement{.tier = Tier::kDram, .writeThrough = Tier::kLocalFs};
  // The names of memo refs start with this.
  std::string refPrefix{"memo/"};
};

// Memos: keys (any bytes, e.g. a plan's fingerprint) to manifests. A memo is
// the ref "<refPrefix><hash of the key>" pointing at the manifest's chunk,
// which also holds the key. Thread safe if the client is; calls fail with
// kiln::Error.
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

 private:
  client::Client& client_;
  const CatalogOptions options_;
};

// A manifest and its key as bytes, versioned, and back. decodeManifest
// throws Error(kInvalidArgument) for bytes it cannot read.
std::string encodeManifest(const std::string& key, const Manifest& manifest);
std::pair<std::string, Manifest> decodeManifest(folly::ByteRange data);

} // namespace kiln::catalog
