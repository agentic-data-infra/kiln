#pragma once

#include <cstdint>
#include <vector>

#include <folly/coro/Task.h>

#include "kiln/common/Types.h"

namespace kiln::rpc {

// The metadata server's operations, independent of fbthrift. The metadata
// server implements them (kiln/meta), and connectMetadata (Rpc.h) calls them
// over the network. Calls fail with kiln::Error.
class MetadataRpc {
 public:
  virtual ~MetadataRpc() = default;

  // A storage server offers its regions (ids unset), of which it lets
  // 'budget' bytes be used (0 for all); returns them with the ids the
  // metadata server gave them.
  virtual folly::coro::Task<std::vector<RegionInfo>> co_registerServer(
      std::vector<RegionInfo> regions,
      uint64_t budget) = 0;

  virtual folly::coro::Task<std::vector<RegionInfo>> co_getRegions(
      std::vector<RegionId> ids) = 0;

  // 'count' free blocks of length kBlockSize in the placement's tier. Evicts
  // the tier's least recently used chunks to make room; fails with
  // kOutOfSpace if that cannot make enough.
  virtual folly::coro::Task<std::vector<BlockRef>> co_allocate(
      Placement placement,
      uint32_t count) = 0;

  // Returns blocks from co_allocate that no chunk uses.
  virtual folly::coro::Task<void> co_free(std::vector<BlockRef> blocks) = 0;

  // Makes a chunk visible. The first commit of an id wins: a later one gets
  // the first one's chunk back, and its own blocks are freed.
  virtual folly::coro::Task<ChunkInfo> co_commit(ChunkInfo chunk) = 0;

  // Fails with kNotFound if there is no such chunk, e.g. it was evicted.
  virtual folly::coro::Task<ChunkInfo> co_lookup(ChunkId id) = 0;

  // Returns whether the chunk existed. Its blocks are freed after the grace
  // period, so readers that looked it up just before can still finish.
  virtual folly::coro::Task<bool> co_remove(ChunkId id) = 0;
};

} // namespace kiln::rpc
