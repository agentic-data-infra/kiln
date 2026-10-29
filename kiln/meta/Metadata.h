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
#include <unordered_set>
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
  // The file that keeps refs, buckets, local-FS regions and written-through
  // chunks across restarts; none if empty, and then all is gone when the metadata
  // server stops.
  std::string journal;
};

// The metadata server's state, in memory: the storage servers' regions and
// budgets, the free blocks in the regions, the copies (replicas) of every
// committed chunk, and refs. Storage servers never hear about chunks; they
// only hold the bytes.
//
// Blocks are allocated round robin over the tier's regions, so a chunk's
// blocks spread over the storage servers; a server hands out at most its
// budget. When a tier is full, allocate evicts the copies there of its least
// recently committed or looked up chunks; a chunk without copies is gone
// (lookups fail with kNotFound). Blocks of dropped copies are reused after
// the grace period. If only blocks in their grace period could satisfy an
// allocation, it waits for them.
//
// With a journal, refs, buckets, local-FS regions and the copies of written-through
// chunks in their target tier are on disk before the call that changes them
// returns, and a restarted metadata server has them back. Copies in regions
// whose storage server has not registered again cannot be read until it
// does.
class Metadata : public rpc::MetadataRpc {
 public:
  // Replays the journal, if any.
  explicit Metadata(MetadataOptions options = {});

  ~Metadata() override;

  // A server's regions come with ids if the server had them registered
  // before (it registers again periodically, and after restarts): the
  // regions it no longer has are dropped with the copies in them, those it
  // reattaches keep their copies, and new regions (id 0) get new ids. All
  // of a server's regions must be in one tier.
  folly::coro::Task<std::vector<RegionInfo>> co_registerServer(
      std::vector<RegionInfo> regions,
      uint64_t budget) override;

  folly::coro::Task<std::vector<RegionInfo>> co_getRegions(
      std::vector<RegionId> ids) override;

  folly::coro::Task<std::vector<BlockRef>> co_allocate(
      Tier tier,
      uint32_t count,
      std::optional<Tier> overflow = std::nullopt) override;

  // Blocks that no reader has seen are reusable at once.
  folly::coro::Task<void> co_free(std::vector<BlockRef> blocks) override;

  folly::coro::Task<ChunkInfo> co_commit(ChunkInfo chunk) override;

  folly::coro::Task<ChunkInfo> co_lookup(ChunkId id) override;

  // Objects of the chunk in an object store stay; the metadata server does
  // not reach object stores.
  folly::coro::Task<bool> co_remove(ChunkId id) override;

  folly::coro::Task<BucketId> co_createBucket() override;

  folly::coro::Task<bool> co_deleteBucket(BucketId bucket) override;

  folly::coro::Task<std::optional<Ref>> co_getRef(std::string name) override;

  folly::coro::Task<std::optional<Ref>> co_casRef(
      std::string name,
      std::optional<uint64_t> expectedVersion,
      std::string value) override;

  folly::coro::Task<std::vector<Ref>> co_listRefs(
      std::string prefix,
      std::string startAfter,
      uint32_t limit) override;

  folly::coro::Task<bool> co_removeRef(
      std::string name,
      std::optional<uint64_t> expectedVersion) override;

  // For tests and status: blocks of 'tier' that allocate can hand out now,
  // and those waiting out their grace period.
  size_t freeBlocks(Tier tier) const;
  size_t graceBlocks(Tier tier) const;

  size_t chunkCount() const;

  // Copies evicted so far.
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

  // A region known from a registration or the journal. Blocks are allocated
  // only in regions of registered servers (in servers_).
  struct Region {
    RegionInfo info;
    // Offsets of free blocks; the last one is handed out first.
    std::vector<uint64_t> free;
  };

  using LruList = std::list<ChunkId>;

  struct Chunk {
    ChunkInfo info;
    // The chunk's place in the LRU list of each tier of blocks it has a
    // copy in.
    LruList::iterator lru[kBlockTiers];
  };

  using ChunkMap = std::unordered_map<ChunkId, Chunk>;

  struct GraceBlock {
    BlockRef block;
    Clock::time_point reusableAt;
  };

  // Tries to allocate under the lock. Returns the blocks, or how long to
  // wait for blocks in their grace period; throws kOutOfSpace if waiting
  // would not help. Evicts to keep some of the tier free if 'evict'.
  std::optional<std::vector<BlockRef>> tryAllocateLocked(
      Tier tier,
      uint32_t count,
      Clock::duration& wait,
      bool evict = true);

  size_t freeBlocksLocked(Tier tier) const;

  // Evicts copies in 'tier' of the least recently used chunks until
  // 'available' blocks (free now or after their grace period) are.
  void evictLocked(Tier tier, uint64_t available);

  // Moves blocks whose grace period is over to the free lists.
  void reclaimLocked(Clock::time_point now);

  // Frees blocks at once, or after the grace period.
  void freeLocked(const std::vector<BlockRef>& blocks, bool grace);

  // Adds a copy to a chunk, journaling it if it is the chunk's copy in its
  // write-through tier.
  void addReplicaLocked(Chunk& chunk, Replica replica);

  // Drops the chunk's copy in 'tier'; erases the chunk if it was the last.
  // Returns the chunk's next iterator in chunks_.
  ChunkMap::iterator dropReplicaLocked(ChunkMap::iterator chunk, Tier tier);

  void eraseChunkLocked(ChunkMap::iterator chunk);

  // Drops a region and the copies with blocks in it.
  void dropRegionLocked(RegionId id);

  // Whether all blocks of the copy are in regions of registered servers.
  bool readableLocked(const Replica& replica) const;

  Server* serverOf(RegionId region);

  // Blocks of 'tier' whose grace period is not over.
  size_t graceBlocksLocked(Tier tier) const;

  LruList& lruOf(Tier tier) {
    return lru_[static_cast<size_t>(tier)];
  }

  // Applies a journal record.
  void replay(folly::ByteRange record);

  // The records that make up the current state, to rewrite the journal.
  std::vector<std::string> snapshot();

  void append(const RecordWriter& record);

  const MetadataOptions options_;

  mutable std::mutex mutex_;
  std::map<std::string, Server> servers_;
  std::map<RegionId, Region> regions_;
  ChunkMap chunks_;
  // The chunks of each bucket.
  std::map<BucketId, std::unordered_set<ChunkId>> buckets_;
  BucketId nextBucket_{1};
  // Per tier of blocks, least recently used first.
  LruList lru_[kBlockTiers];
  // In the order they were freed, so in order of reusableAt.
  std::deque<GraceBlock> grace_;
  // Where the next allocation starts its round over the regions.
  RegionId nextAllocation_{0};
  size_t evictions_{0};

  std::mutex refMutex_;
  std::map<std::string, Ref> refs_;
  // The version of the next update of any ref.
  uint64_t nextVersion_{1};

  // Taken after mutex_ or refMutex_.
  std::mutex journalMutex_;
  std::unique_ptr<Journal> journal_;
};

} // namespace kiln::meta
