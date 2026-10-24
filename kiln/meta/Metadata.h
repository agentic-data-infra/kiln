#pragma once

#include <chrono>
#include <deque>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "kiln/meta/Journal.h"
#include "kiln/rpc/MetadataRpc.h"

namespace kiln::meta {

struct MetadataOptions {
  // How long the blocks of a removed or evicted chunk stay untouched before
  // they are reused. Clients cache chunk infos for less than this
  // (ClientOptions::chunkCacheTtl), so a reader that found the chunk just
  // before still reads its bytes.
  std::chrono::milliseconds gracePeriod{std::chrono::seconds(30)};
  // When an allocation would leave less than 'evictBelow' of a tier's
  // budget free (counting blocks in their grace period as free), allocate
  // evicts least recently used chunks until 'evictUntil' of it is free, so
  // that later allocations find blocks whose grace period is over.
  double evictBelow{0.1};
  double evictUntil{0.2};
  // The file that keeps refs across restarts; none if empty, and then refs
  // are gone when the metadata server stops.
  std::string journal;
};

// The metadata server's state, in memory: the storage servers' regions and
// budgets, the free blocks in the regions, and the blocks of every committed
// chunk. Storage servers never hear about chunks; they only hold the bytes.
//
// Blocks are allocated round robin over the tier's regions, so a chunk's
// blocks spread over the storage servers; a server hands out at most its
// budget. When a tier is full, allocate evicts its least recently committed
// or looked up chunks: they are gone (lookups fail with kNotFound), and
// their blocks are reused after the grace period. If only blocks in their
// grace period could satisfy an allocation, it waits for them.
//
// Refs are kept apart from the blocks, and with a journal every update is
// on disk before the call returns.
class Metadata : public rpc::MetadataRpc {
 public:
  // Replays the journal, if any.
  explicit Metadata(MetadataOptions options = {});

  ~Metadata() override;

  // A storage server that registers again (e.g. after a restart) replaces
  // its old regions; chunks with blocks in them are gone, as the tiers are
  // ephemeral. All of a server's regions must be in one tier.
  folly::coro::Task<std::vector<RegionInfo>> co_registerServer(
      std::vector<RegionInfo> regions,
      uint64_t budget) override;

  folly::coro::Task<std::vector<RegionInfo>> co_getRegions(
      std::vector<RegionId> ids) override;

  folly::coro::Task<std::vector<BlockRef>> co_allocate(
      Placement placement,
      uint32_t count) override;

  // Blocks that no reader has seen are reusable at once.
  folly::coro::Task<void> co_free(std::vector<BlockRef> blocks) override;

  folly::coro::Task<ChunkInfo> co_commit(ChunkInfo chunk) override;

  folly::coro::Task<ChunkInfo> co_lookup(ChunkId id) override;

  folly::coro::Task<bool> co_remove(ChunkId id) override;

  folly::coro::Task<std::optional<Ref>> co_getRef(std::string name) override;

  folly::coro::Task<std::optional<Ref>> co_casRef(
      std::string name,
      std::optional<uint64_t> expectedVersion,
      std::string value) override;

  folly::coro::Task<std::vector<Ref>> co_listRefs(std::string prefix) override;

  folly::coro::Task<bool> co_removeRef(
      std::string name,
      std::optional<uint64_t> expectedVersion) override;

  // For tests and status: blocks of 'tier' that allocate can hand out now,
  // and those waiting out their grace period.
  size_t freeBlocks(Tier tier) const;
  size_t graceBlocks(Tier tier) const;

  size_t chunkCount() const;

  // Chunks evicted so far.
  size_t evictions() const;

 private:
  using Clock = std::chrono::steady_clock;

  struct Server {
    Tier tier;
    uint64_t budgetBlocks;
    // Handed out, in chunks or not yet committed.
    uint64_t liveBlocks{0};
    // Freed, in their grace period.
    uint64_t graceBlocks{0};
  };

  struct Region {
    RegionInfo info;
    // Offsets of free blocks; the last one is handed out first.
    std::vector<uint64_t> free;
  };

  struct Chunk {
    ChunkInfo info;
    // The chunk's place in its tier's LRU list.
    std::list<ChunkId>::iterator lru;
  };

  struct GraceBlock {
    BlockRef block;
    Clock::time_point reusableAt;
  };

  // Tries to allocate under the lock. Returns the blocks, or how long to
  // wait for blocks in their grace period; throws kOutOfSpace if waiting
  // would not help.
  std::optional<std::vector<BlockRef>> tryAllocateLocked(
      Placement placement,
      uint32_t count,
      Clock::duration& wait);

  // Evicts least recently used chunks of 'tier' until 'free' blocks (free
  // now or after their grace period) are available.
  void evictLocked(Tier tier, uint64_t free);

  // Moves blocks whose grace period is over to the free lists.
  void reclaimLocked(Clock::time_point now);

  // Frees blocks at once, or after the grace period.
  void freeLocked(const std::vector<BlockRef>& blocks, bool grace);

  void eraseChunkLocked(
      std::unordered_map<ChunkId, Chunk>::iterator chunk,
      bool grace);

  Server* serverOf(RegionId region);

  // Blocks of 'tier' whose grace period is not over.
  size_t graceBlocksLocked(Tier tier) const;

  std::list<ChunkId>& lruOf(Tier tier) {
    return lru_[static_cast<size_t>(tier)];
  }

  const MetadataOptions options_;

  mutable std::mutex mutex_;
  std::map<std::string, Server> servers_;
  std::map<RegionId, Region> regions_;
  std::unordered_map<ChunkId, Chunk> chunks_;
  // Per tier, least recently used first.
  std::list<ChunkId> lru_[2];
  // In the order they were freed, so in order of reusableAt.
  std::deque<GraceBlock> grace_;
  RegionId nextRegionId_{1};
  // Where the next allocation starts its round over the regions.
  RegionId nextAllocation_{0};
  size_t evictions_{0};

  // Applies a journal record.
  void replayLocked(folly::ByteRange record);

  void appendLocked(const RecordWriter& record);

  std::mutex refMutex_;
  std::map<std::string, Ref> refs_;
  // The version of the next update of any ref.
  uint64_t nextVersion_{1};
  std::unique_ptr<Journal> journal_;
};

} // namespace kiln::meta
