#pragma once

#include <map>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "kiln/rpc/MetadataRpc.h"

namespace kiln::meta {

// The metadata server's state, in memory: the storage servers' regions, the
// free blocks in them, and the blocks of every committed chunk. Storage
// servers never hear about chunks; they only hold the bytes.
//
// Blocks are allocated round robin over the tier's regions, so a chunk's
// blocks spread over the storage servers. Freed blocks are reusable at once
// (eviction with a grace period comes later).
class Metadata : public rpc::MetadataRpc {
 public:
  // A storage server that registers again (e.g. after a restart) replaces
  // its old regions; chunks with blocks in them are gone, as the tiers are
  // ephemeral.
  folly::coro::Task<std::vector<RegionInfo>> co_registerServer(
      std::vector<RegionInfo> regions) override;

  folly::coro::Task<std::vector<RegionInfo>> co_getRegions(
      std::vector<RegionId> ids) override;

  folly::coro::Task<std::vector<BlockRef>> co_allocate(
      Placement placement,
      uint32_t count) override;

  folly::coro::Task<void> co_free(std::vector<BlockRef> blocks) override;

  folly::coro::Task<ChunkInfo> co_commit(ChunkInfo chunk) override;

  folly::coro::Task<ChunkInfo> co_lookup(ChunkId id) override;

  folly::coro::Task<bool> co_remove(ChunkId id) override;

  // The free blocks in 'tier', for tests and status.
  size_t freeBlocks(Tier tier) const;

  size_t chunkCount() const;

 private:
  struct Region {
    RegionInfo info;
    // Offsets of free blocks; the last one is handed out first.
    std::vector<uint64_t> free;
  };

  void freeLocked(const std::vector<BlockRef>& blocks);

  mutable std::mutex mutex_;
  std::map<RegionId, Region> regions_;
  std::unordered_map<ChunkId, ChunkInfo> chunks_;
  RegionId nextRegionId_{1};
  // Where the next allocation starts its round over the regions.
  RegionId nextAllocation_{0};
};

} // namespace kiln::meta
