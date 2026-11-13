#include <gtest/gtest.h>

#include <folly/coro/BlockingWait.h>
#include <folly/testing/TestUtil.h>

#include "kiln/common/Error.h"
#include "kiln/meta/Metadata.h"

namespace kiln::meta {
namespace {

using folly::coro::blockingWait;
using namespace std::chrono_literals;

// Registers a server with one region per entry of 'regionBlocks'.
std::vector<RegionInfo> registerServer(
    Metadata& metadata,
    const std::string& server,
    Tier tier,
    std::vector<uint64_t> regionBlocks,
    uint64_t budgetBlocks = 0) {
  std::vector<RegionInfo> regions;
  for (auto blocks : regionBlocks) {
    regions.push_back(RegionInfo{
        .server = server, .tier = tier, .length = blocks * kBlockSize});
  }
  return blockingWait(
      metadata.co_registerServer(std::move(regions), budgetBlocks * kBlockSize));
}

// Commits a chunk of 'blocks' full blocks in 'tier', ephemeral unless
// asked: most tests are about eviction.
ChunkId commit(
    Metadata& metadata,
    uint64_t id,
    uint32_t blocks,
    Tier tier = Tier::kDram,
    BucketId bucket = kDefaultBucket,
    Retention retention = Retention::kEphemeral) {
  ChunkInfo chunk{
      .id = ChunkId{id, id},
      .bucket = bucket,
      .size = blocks * kBlockSize,
      .placement = Placement{.tier = tier, .retention = retention},
      .replicas = {Replica{
          .tier = tier,
          .blocks = blockingWait(metadata.co_allocate(tier, blocks))}}};
  return blockingWait(metadata.co_commit(std::move(chunk))).id;
}

ChunkId commitRetained(
    Metadata& metadata,
    uint64_t id,
    uint32_t blocks,
    Tier tier = Tier::kDram) {
  return commit(
      metadata, id, blocks, tier, kDefaultBucket, Retention::kRetained);
}

// The tiers of the chunk's copies.
std::vector<Tier> tiersOf(Metadata& metadata, ChunkId id) {
  std::vector<Tier> tiers;
  for (const auto& replica : blockingWait(metadata.co_lookup(id)).replicas) {
    tiers.push_back(replica.tier);
  }
  return tiers;
}

bool exists(Metadata& metadata, ChunkId id) {
  try {
    blockingWait(metadata.co_lookup(id));
    return true;
  } catch (const Error& e) {
    EXPECT_EQ(e.code(), ErrorCode::kNotFound);
    return false;
  }
}

ErrorCode errorOf(folly::coro::Task<void> call) {
  try {
    blockingWait(std::move(call));
  } catch (const Error& e) {
    return e.code();
  }
  ADD_FAILURE() << "No error";
  return ErrorCode::kInternal;
}

// No grace period and no eviction ahead of need, unless a test asks.
MetadataOptions exact() {
  return MetadataOptions{.gracePeriod = 0ms, .evictBelow = 0, .evictUntil = 0};
}

TEST(MetadataTest, registerAssignsIds) {
  Metadata metadata;
  auto regions = registerServer(metadata, "a:1", Tier::kDram, {4, 2});
  ASSERT_EQ(regions.size(), 2);
  EXPECT_NE(regions[0].id, regions[1].id);
  EXPECT_EQ(
      blockingWait(metadata.co_getRegions({regions[1].id})),
      std::vector{regions[1]});
  EXPECT_EQ(metadata.freeBlocks(Tier::kDram), 6);
  EXPECT_EQ(metadata.freeBlocks(Tier::kLocalFs), 0);
  EXPECT_EQ(
      errorOf([&]() -> folly::coro::Task<void> {
        co_await metadata.co_getRegions({12345});
      }()),
      ErrorCode::kNotFound);
  // One tier per server.
  std::vector<RegionInfo> mixed{
      RegionInfo{.server = "b:1", .tier = Tier::kDram, .length = kBlockSize},
      RegionInfo{.server = "b:1", .tier = Tier::kLocalFs, .length = kBlockSize}};
  EXPECT_EQ(
      errorOf([&]() -> folly::coro::Task<void> {
        co_await metadata.co_registerServer(mixed, 0);
      }()),
      ErrorCode::kInvalidArgument);
}

TEST(MetadataTest, allocateRoundRobin) {
  Metadata metadata(exact());
  registerServer(metadata, "a:1", Tier::kDram, {4});
  registerServer(metadata, "b:1", Tier::kDram, {4});
  registerServer(metadata, "c:1", Tier::kLocalFs, {4});

  auto blocks = blockingWait(metadata.co_allocate(Tier::kDram, 6));
  ASSERT_EQ(blocks.size(), 6);
  std::map<RegionId, int> perRegion;
  for (const auto& block : blocks) {
    EXPECT_EQ(block.length, kBlockSize);
    EXPECT_EQ(block.offset % kBlockSize, 0);
    ++perRegion[block.region];
  }
  EXPECT_EQ(perRegion.size(), 2);
  for (auto [id, count] : perRegion) {
    EXPECT_EQ(count, 3);
  }
  EXPECT_EQ(metadata.freeBlocks(Tier::kDram), 2);

  // Blocks being written cannot be evicted.
  EXPECT_EQ(
      errorOf([&]() -> folly::coro::Task<void> {
        co_await metadata.co_allocate(Tier::kDram, 3);
      }()),
      ErrorCode::kOutOfSpace);
  blockingWait(metadata.co_free(blocks));
  EXPECT_EQ(metadata.freeBlocks(Tier::kDram), 8);
  EXPECT_EQ(metadata.graceBlocks(Tier::kDram), 0);
}

TEST(MetadataTest, stickyAllocationsStayOnOneServer) {
  Metadata metadata(exact());
  std::map<RegionId, std::string> serverOf;
  for (const auto* server : {"a:1", "b:1", "c:1"}) {
    for (const auto& region : registerServer(metadata, server, Tier::kDram, {2, 2})) {
      serverOf[region.id] = region.server;
    }
  }
  auto servers = [&](const std::vector<BlockRef>& blocks) {
    std::map<std::string, int> count;
    for (const auto& block : blocks) {
      ++count[serverOf.at(block.region)];
    }
    return count;
  };

  // Each sticky allocation on one server (over its regions), round robin.
  std::set<std::string> used;
  std::vector<BlockRef> firsts;
  for (int i = 0; i < 3; ++i) {
    auto blocks = blockingWait(metadata.co_allocate(Tier::kDram, 3, std::nullopt, Stick{}));
    auto on = servers(blocks);
    ASSERT_EQ(on.size(), 1);
    EXPECT_EQ(on.begin()->second, 3);
    used.insert(on.begin()->first);
    firsts.push_back(blocks[0]);
  }
  EXPECT_EQ(used.size(), 3);
  // Sticking to a region's server: its last block, then elsewhere.
  auto more = blockingWait(metadata.co_allocate(
      Tier::kDram, 2, std::nullopt, Stick{.region = firsts[0].region}));
  auto on = servers(more);
  EXPECT_EQ(on[serverOf.at(firsts[0].region)], 1);
  EXPECT_EQ(on.size(), 2);
}

TEST(MetadataTest, stickyAllocationsPreferAHost) {
  Metadata metadata(exact());
  // Servers a:1 and a:2 on host "a", b:1 on "b"; two regions of 2 blocks each.
  std::map<RegionId, std::string> serverOf;
  for (auto [server, host] : {std::pair{"a:1", "a"}, {"a:2", "a"}, {"b:1", "b"}}) {
    std::vector<RegionInfo> regions;
    for (int i = 0; i < 2; ++i) {
      regions.push_back(RegionInfo{
          .server = server,
          .tier = Tier::kDram,
          .length = 2 * kBlockSize,
          .shm = ShmAccess{.host = host, .path = "/dev/shm/x"}});
    }
    for (const auto& region :
         blockingWait(metadata.co_registerServer(std::move(regions), 0))) {
      serverOf[region.id] = region.server;
    }
  }
  auto allocate = [&](uint32_t count, std::string host) {
    std::map<std::string, int> on;
    for (const auto& block : blockingWait(metadata.co_allocate(
             Tier::kDram, count, std::nullopt, Stick{.host = host}))) {
      ++on[serverOf.at(block.region)];
    }
    return on;
  };

  // On "b" while it has room for all the blocks, on one server.
  EXPECT_EQ(allocate(3, "b"), (std::map<std::string, int>{{"b:1", 3}}));
  // "b" has one block left: a server on "a" has room for both.
  auto on = allocate(2, "b");
  EXPECT_EQ(on.size(), 1);
  EXPECT_TRUE(on.contains("a:1") || on.contains("a:2"));
  // A host without servers: round robin, still one server per allocation.
  EXPECT_EQ(allocate(2, "c").size(), 1);
  // The last free blocks, wherever they are: the write succeeds.
  EXPECT_EQ(allocate(5, "b").size(), 3);
}

TEST(MetadataTest, budgetLimitsAllocation) {
  Metadata metadata(exact());
  // 8 blocks of regions, 3 of them usable.
  registerServer(metadata, "a:1", Tier::kDram, {4, 4}, 3);
  EXPECT_EQ(metadata.freeBlocks(Tier::kDram), 3);
  auto blocks = blockingWait(metadata.co_allocate(Tier::kDram, 3));
  EXPECT_EQ(metadata.freeBlocks(Tier::kDram), 0);
  EXPECT_EQ(
      errorOf([&]() -> folly::coro::Task<void> {
        co_await metadata.co_allocate(Tier::kDram, 1);
      }()),
      ErrorCode::kOutOfSpace);
  blockingWait(metadata.co_free(blocks));
  EXPECT_EQ(metadata.freeBlocks(Tier::kDram), 3);
}

TEST(MetadataTest, commitLookupRemove) {
  Metadata metadata(exact());
  registerServer(metadata, "a:1", Tier::kDram, {4});
  auto blocks = blockingWait(metadata.co_allocate(Tier::kDram, 2));
  blocks[1].length = 10;
  const ChunkInfo chunk{
      .id = ChunkId{1, 2},
      .size = kBlockSize + 10,
      .replicas = {Replica{.blocks = blocks}}};

  EXPECT_EQ(blockingWait(metadata.co_commit(chunk)), chunk);
  EXPECT_EQ(blockingWait(metadata.co_lookup(chunk.id)), chunk);

  // A second commit of the id gets the first chunk; its blocks are free at
  // once, as nobody has seen them.
  auto other = blockingWait(metadata.co_allocate(Tier::kDram, 2));
  other[1].length = 10;
  auto duplicate = chunk;
  duplicate.replicas[0].blocks = other;
  EXPECT_EQ(metadata.freeBlocks(Tier::kDram), 0);
  EXPECT_EQ(blockingWait(metadata.co_commit(duplicate)), chunk);
  EXPECT_EQ(metadata.freeBlocks(Tier::kDram), 2);

  EXPECT_TRUE(blockingWait(metadata.co_remove(chunk.id)));
  EXPECT_FALSE(blockingWait(metadata.co_remove(chunk.id)));
  EXPECT_EQ(metadata.freeBlocks(Tier::kDram), 4);
  EXPECT_FALSE(exists(metadata, chunk.id));
}

TEST(MetadataTest, commitChecksBlocks) {
  Metadata metadata;
  registerServer(metadata, "a:1", Tier::kDram, {4});
  auto blocks = blockingWait(metadata.co_allocate(Tier::kDram, 1));
  ChunkInfo wrongSize{
      .id = ChunkId{1, 2}, .size = 5, .replicas = {Replica{.blocks = blocks}}};
  EXPECT_EQ(
      errorOf([&]() -> folly::coro::Task<void> {
        co_await metadata.co_commit(wrongSize);
      }()),
      ErrorCode::kInvalidArgument);
  ChunkInfo noRegion{
      .id = ChunkId{1, 2},
      .size = 5,
      .replicas = {
          Replica{.blocks = {BlockRef{.region = 999, .length = 5}}}}};
  EXPECT_EQ(
      errorOf([&]() -> folly::coro::Task<void> {
        co_await metadata.co_commit(noRegion);
      }()),
      ErrorCode::kInvalidArgument);
  ChunkInfo wrongTier{
      .id = ChunkId{1, 2},
      .size = kBlockSize,
      .placement = Placement{.tier = Tier::kLocalFs},
      .replicas = {Replica{.tier = Tier::kLocalFs, .blocks = blocks}}};
  EXPECT_EQ(
      errorOf([&]() -> folly::coro::Task<void> {
        co_await metadata.co_commit(wrongTier);
      }()),
      ErrorCode::kInvalidArgument);
  // Writing through to DRAM would journal a copy that a restart loses.
  ChunkInfo notDurable{
      .id = ChunkId{1, 2},
      .size = kBlockSize,
      .placement = Placement{.tier = Tier::kLocalFs, .writeThrough = Tier::kDram},
      .replicas = {
          Replica{.tier = Tier::kDram, .blocks = blocks},
          Replica{.tier = Tier::kLocalFs, .blocks = blocks}}};
  EXPECT_EQ(
      errorOf([&]() -> folly::coro::Task<void> {
        co_await metadata.co_commit(notDurable);
      }()),
      ErrorCode::kInvalidArgument);
}

TEST(MetadataTest, evictsLeastRecentlyUsed) {
  Metadata metadata(exact());
  registerServer(metadata, "a:1", Tier::kDram, {10});
  registerServer(metadata, "b:1", Tier::kLocalFs, {10});
  const auto a = commit(metadata, 1, 3);
  const auto b = commit(metadata, 2, 3);
  const auto c = commit(metadata, 3, 3);
  const auto other = commit(metadata, 4, 9, Tier::kLocalFs);
  // a is used again, so b is now the least recently used.
  blockingWait(metadata.co_lookup(a));

  const auto d = commit(metadata, 5, 3);
  EXPECT_EQ(metadata.evictions(), 1);
  EXPECT_TRUE(exists(metadata, a));
  EXPECT_FALSE(exists(metadata, b));
  EXPECT_TRUE(exists(metadata, c));
  EXPECT_TRUE(exists(metadata, d));
  // Eviction stays in its tier.
  EXPECT_TRUE(exists(metadata, other));

  // A chunk bigger than the tier cannot be made room for.
  EXPECT_EQ(
      errorOf([&]() -> folly::coro::Task<void> {
        co_await metadata.co_allocate(Tier::kDram, 11);
      }()),
      ErrorCode::kOutOfSpace);
}

TEST(MetadataTest, evictsAheadOfNeed) {
  Metadata metadata(MetadataOptions{
      .gracePeriod = 0ms, .evictBelow = 0.25, .evictUntil = 0.5});
  registerServer(metadata, "a:1", Tier::kDram, {8});
  for (uint64_t i = 1; i <= 4; ++i) {
    commit(metadata, i, 1);
  }
  // Allocating 3 of the 4 free blocks would leave less than a quarter
  // free: evict until 3 + half the budget are available.
  blockingWait(metadata.co_allocate(Tier::kDram, 3));
  EXPECT_EQ(metadata.evictions(), 3);
  EXPECT_FALSE(exists(metadata, ChunkId{3, 3}));
  EXPECT_TRUE(exists(metadata, ChunkId{4, 4}));
}

TEST(MetadataTest, overflowInsteadOfEvicting) {
  Metadata metadata(exact());
  registerServer(metadata, "a:1", Tier::kDram, {4});
  registerServer(metadata, "b:1", Tier::kLocalFs, {4});

  // Free DRAM blocks come first.
  auto blocks = blockingWait(metadata.co_allocate(Tier::kDram, 3, Tier::kLocalFs));
  EXPECT_EQ(metadata.freeBlocks(Tier::kDram), 1);
  EXPECT_EQ(metadata.freeBlocks(Tier::kLocalFs), 4);
  blockingWait(metadata.co_free(std::move(blocks)));

  // A committed chunk fills DRAM: overflow neither evicts it nor splits
  // the allocation, but takes all blocks in local FS.
  const auto chunk = commit(metadata, 1, 3);
  blocks = blockingWait(metadata.co_allocate(Tier::kDram, 2, Tier::kLocalFs));
  EXPECT_TRUE(exists(metadata, chunk));
  EXPECT_EQ(metadata.freeBlocks(Tier::kDram), 1);
  EXPECT_EQ(metadata.freeBlocks(Tier::kLocalFs), 2);
  auto regions = blockingWait(metadata.co_getRegions({blocks[0].region}));
  EXPECT_EQ(regions[0].tier, Tier::kLocalFs);

  // A full overflow tier evicts as usual.
  commit(metadata, 2, 2, Tier::kLocalFs);
  blockingWait(metadata.co_allocate(Tier::kDram, 2, Tier::kLocalFs));
  EXPECT_EQ(metadata.evictions(), 1);
  EXPECT_TRUE(exists(metadata, chunk));

  EXPECT_EQ(
      errorOf([&]() -> folly::coro::Task<void> {
        co_await metadata.co_allocate(Tier::kDram, 1, Tier::kObjectStore);
      }()),
      ErrorCode::kInvalidArgument);
}

TEST(MetadataTest, gracePeriodDelaysReuse) {
  Metadata metadata(MetadataOptions{
      .gracePeriod = 300ms, .evictBelow = 0, .evictUntil = 0});
  registerServer(metadata, "a:1", Tier::kDram, {4});
  const auto chunk = commit(metadata, 1, 4);
  EXPECT_TRUE(blockingWait(metadata.co_remove(chunk)));
  EXPECT_EQ(metadata.freeBlocks(Tier::kDram), 0);
  EXPECT_EQ(metadata.graceBlocks(Tier::kDram), 4);

  // The allocation waits for the grace period instead of failing.
  const auto start = std::chrono::steady_clock::now();
  auto blocks = blockingWait(metadata.co_allocate(Tier::kDram, 2));
  EXPECT_EQ(blocks.size(), 2);
  EXPECT_GE(std::chrono::steady_clock::now() - start, 250ms);
  EXPECT_EQ(metadata.graceBlocks(Tier::kDram), 0);
  EXPECT_EQ(metadata.freeBlocks(Tier::kDram), 2);
}

TEST(MetadataTest, evictedBlocksWaitOutTheGracePeriod) {
  Metadata metadata(MetadataOptions{
      .gracePeriod = 300ms, .evictBelow = 0, .evictUntil = 0});
  registerServer(metadata, "a:1", Tier::kDram, {4});
  const auto old = commit(metadata, 1, 4);
  const auto start = std::chrono::steady_clock::now();
  commit(metadata, 2, 2);
  EXPECT_GE(std::chrono::steady_clock::now() - start, 250ms);
  EXPECT_FALSE(exists(metadata, old));
  EXPECT_EQ(metadata.evictions(), 1);
}

TEST(MetadataTest, reregisteringDropsOldRegionsAndChunks) {
  Metadata metadata(exact());
  registerServer(metadata, "a:1", Tier::kDram, {4});
  commit(metadata, 1, 1);
  EXPECT_EQ(metadata.chunkCount(), 1);

  registerServer(metadata, "a:1", Tier::kDram, {2});
  EXPECT_EQ(metadata.chunkCount(), 0);
  EXPECT_EQ(metadata.freeBlocks(Tier::kDram), 2);
}

TEST(MetadataTest, writeThroughCopies) {
  Metadata metadata(exact());
  registerServer(metadata, "a:1", Tier::kDram, {4});
  registerServer(metadata, "b:1", Tier::kLocalFs, {4});
  const Placement placement{.tier = Tier::kDram, .writeThrough = Tier::kLocalFs};
  ChunkInfo chunk{
      .id = ChunkId{1, 1},
      .size = 2 * kBlockSize,
      .placement = placement,
      .replicas = {
          Replica{
              .tier = Tier::kDram,
              .blocks = blockingWait(metadata.co_allocate(Tier::kDram, 2))},
          Replica{
              .tier = Tier::kLocalFs,
              .blocks = blockingWait(metadata.co_allocate(Tier::kLocalFs, 2))}}};
  EXPECT_EQ(blockingWait(metadata.co_commit(chunk)), chunk);

  // Evicting the DRAM copy leaves the local-FS one, also of a retained
  // chunk.
  EXPECT_EQ(chunk.placement.retention, Retention::kRetained);
  commit(metadata, 2, 4);
  auto left = blockingWait(metadata.co_lookup(chunk.id));
  ASSERT_EQ(left.replicas.size(), 1);
  EXPECT_EQ(left.replicas[0], chunk.replicas[1]);
  EXPECT_EQ(metadata.evictions(), 1);

  // Commits need a copy in each tier of the placement.
  chunk.id = ChunkId{3, 3};
  chunk.replicas.pop_back();
  EXPECT_EQ(
      errorOf([&]() -> folly::coro::Task<void> {
        co_await metadata.co_commit(chunk);
      }()),
      ErrorCode::kInvalidArgument);
  EXPECT_EQ(
      errorOf([&]() -> folly::coro::Task<void> {
        co_await metadata.co_allocate(Tier::kObjectStore, 1);
      }()),
      ErrorCode::kInvalidArgument);
}

TEST(MetadataTest, registeringAgainKeepsRegionsWithIds) {
  Metadata metadata(exact());
  auto regions = registerServer(metadata, "a:1", Tier::kLocalFs, {4});
  const auto id = commit(metadata, 1, 3, Tier::kLocalFs);
  auto blocks = blockingWait(metadata.co_allocate(Tier::kLocalFs, 1));
  EXPECT_EQ(metadata.freeBlocks(Tier::kLocalFs), 0);

  // Same regions, maybe after a restart on another port.
  for (auto& region : regions) {
    region.server = "a:2";
  }
  blockingWait(metadata.co_registerServer(regions, 0));
  EXPECT_TRUE(exists(metadata, id));
  EXPECT_EQ(metadata.freeBlocks(Tier::kLocalFs), 0);
  blockingWait(metadata.co_free(blocks));
  EXPECT_EQ(metadata.freeBlocks(Tier::kLocalFs), 1);

  // New regions: the chunk in the old ones is gone.
  registerServer(metadata, "a:2", Tier::kLocalFs, {4});
  EXPECT_FALSE(exists(metadata, id));
  EXPECT_EQ(metadata.freeBlocks(Tier::kLocalFs), 4);
}

TEST(MetadataTest, journalKeepsWrittenThroughCopies) {
  folly::test::TemporaryDirectory dir;
  MetadataOptions options = exact();
  options.journal = dir.path().string() + "/journal";
  std::vector<RegionInfo> regions;
  ChunkInfo durable;
  ChunkInfo object;
  ChunkId ephemeral;
  {
    Metadata metadata(options);
    registerServer(metadata, "a:1", Tier::kDram, {4});
    regions = registerServer(metadata, "b:1", Tier::kLocalFs, {4});
    durable = ChunkInfo{
        .id = ChunkId{1, 1},
        .size = kBlockSize,
        .placement = {.tier = Tier::kDram, .writeThrough = Tier::kLocalFs},
        .replicas = {
            Replica{
                .tier = Tier::kDram,
                .blocks = blockingWait(metadata.co_allocate(Tier::kDram, 1))},
            Replica{
                .tier = Tier::kLocalFs,
                .blocks =
                    blockingWait(metadata.co_allocate(Tier::kLocalFs, 1))}}};
    blockingWait(metadata.co_commit(durable));
    object = ChunkInfo{
        .id = ChunkId{2, 2},
        .size = kBlockSize,
        .placement = {.tier = Tier::kDram, .writeThrough = Tier::kObjectStore},
        .replicas = {
            Replica{
                .tier = Tier::kDram,
                .blocks = blockingWait(metadata.co_allocate(Tier::kDram, 1))},
            Replica{.tier = Tier::kObjectStore}}};
    blockingWait(metadata.co_commit(object));
    ephemeral = commit(metadata, 3, 2, Tier::kLocalFs);
    // Removed: not back after the restart.
    blockingWait(metadata.co_remove(commit(metadata, 4, 1, Tier::kLocalFs)));
  }

  Metadata metadata(options);
  EXPECT_EQ(metadata.chunkCount(), 2);
  EXPECT_EQ(
      blockingWait(metadata.co_lookup(object.id)).replicas,
      std::vector{object.replicas[1]});
  // Not readable before its storage server registers again.
  EXPECT_EQ(
      errorOf([&]() -> folly::coro::Task<void> {
        co_await metadata.co_lookup(durable.id);
      }()),
      ErrorCode::kUnavailable);
  blockingWait(metadata.co_registerServer(regions, 0));
  EXPECT_EQ(
      blockingWait(metadata.co_lookup(durable.id)).replicas,
      std::vector{durable.replicas[1]});
  EXPECT_FALSE(exists(metadata, ephemeral));
  EXPECT_EQ(metadata.freeBlocks(Tier::kLocalFs), 3);
  // The block in use is not handed out.
  for (const auto& block : blockingWait(metadata.co_allocate(Tier::kLocalFs, 3))) {
    EXPECT_NE(block, durable.replicas[1].blocks[0]);
  }
}

TEST(MetadataTest, retainedChunksAreNotEvicted) {
  Metadata metadata(exact());
  registerServer(metadata, "a:1", Tier::kDram, {4});
  const auto kept = commitRetained(metadata, 1, 2);
  const auto cached = commit(metadata, 2, 2);
  // The ephemeral chunk makes room; the retained one stays.
  commit(metadata, 3, 2);
  EXPECT_EQ(metadata.evictions(), 1);
  EXPECT_FALSE(exists(metadata, cached));
  EXPECT_TRUE(exists(metadata, kept));

  // A tier full of retained chunks has no room.
  blockingWait(metadata.co_remove(ChunkId{3, 3}));
  commitRetained(metadata, 4, 2);
  EXPECT_EQ(
      errorOf([&]() -> folly::coro::Task<void> {
        co_await metadata.co_allocate(Tier::kDram, 1);
      }()),
      ErrorCode::kOutOfSpace);
  EXPECT_TRUE(exists(metadata, kept));
  EXPECT_EQ(metadata.evictions(), 1);
}

TEST(MetadataTest, retainedCopiesGoOnInTheOverflowTier) {
  Metadata metadata(exact());
  registerServer(metadata, "a:1", Tier::kDram, {4});
  registerServer(metadata, "b:1", Tier::kLocalFs, {4});
  auto copy = [&](std::vector<Tier> tiers) {
    Replica replica{.tier = tiers[0]};
    for (auto tier : tiers) {
      auto blocks = blockingWait(metadata.co_allocate(tier, 1));
      replica.blocks.push_back(blocks[0]);
    }
    return replica;
  };
  auto chunk = [&](uint64_t id, Replica replica, Placement placement) {
    return ChunkInfo{
        .id = ChunkId{id, id},
        .size = replica.blocks.size() * kBlockSize,
        .placement = placement,
        .replicas = {std::move(replica)}};
  };
  const Placement retained{.tier = Tier::kDram};
  const Placement ephemeral{
      .tier = Tier::kDram, .retention = Retention::kEphemeral};

  // A retained chunk's copy may start in DRAM and go on in local FS.
  const auto mixed =
      chunk(1, copy({Tier::kDram, Tier::kLocalFs, Tier::kLocalFs}), retained);
  EXPECT_EQ(blockingWait(metadata.co_commit(mixed)), mixed);
  EXPECT_EQ(blockingWait(metadata.co_lookup(mixed.id)).replicas, mixed.replicas);

  // Not an ephemeral one, nor back and forth.
  for (const auto& bad :
       {chunk(2, copy({Tier::kDram, Tier::kLocalFs}), ephemeral),
        chunk(3, copy({Tier::kDram, Tier::kLocalFs, Tier::kDram}), retained)}) {
    EXPECT_EQ(
        errorOf([&]() -> folly::coro::Task<void> {
          co_await metadata.co_commit(bad);
        }()),
        ErrorCode::kInvalidArgument);
  }

  // Removing it frees its blocks in both tiers (the rejected chunks' blocks
  // stay allocated).
  EXPECT_TRUE(blockingWait(metadata.co_remove(mixed.id)));
  EXPECT_EQ(metadata.freeBlocks(Tier::kDram), 1);
  EXPECT_EQ(metadata.freeBlocks(Tier::kLocalFs), 2);
}

TEST(MetadataTest, releasedBucketsHoldEphemeralChunks) {
  Metadata metadata(exact());
  registerServer(metadata, "a:1", Tier::kDram, {4});
  const auto job = blockingWait(metadata.co_createBucket());
  const auto output =
      commit(metadata, 1, 2, Tier::kDram, job, Retention::kRetained);
  const auto kept = commitRetained(metadata, 2, 2);
  EXPECT_EQ(
      errorOf([&]() -> folly::coro::Task<void> {
        co_await metadata.co_allocate(Tier::kDram, 1);
      }()),
      ErrorCode::kOutOfSpace);

  EXPECT_TRUE(blockingWait(metadata.co_releaseBucket(job)));
  EXPECT_EQ(
      blockingWait(metadata.co_lookup(output)).placement.retention,
      Retention::kEphemeral);
  // Chunks committed into it later are ephemeral too.
  EXPECT_TRUE(blockingWait(metadata.co_remove(kept)));
  const auto late =
      commit(metadata, 3, 2, Tier::kDram, job, Retention::kRetained);
  EXPECT_EQ(
      blockingWait(metadata.co_lookup(late)).placement.retention,
      Retention::kEphemeral);
  // The job's chunks make room for a retained one.
  commitRetained(metadata, 4, 2);
  EXPECT_EQ(metadata.evictions(), 1);
  EXPECT_FALSE(exists(metadata, output));

  EXPECT_FALSE(blockingWait(metadata.co_releaseBucket(job + 1)));
  EXPECT_EQ(
      errorOf([&]() -> folly::coro::Task<void> {
        co_await metadata.co_releaseBucket(kDefaultBucket);
      }()),
      ErrorCode::kInvalidArgument);
}

TEST(MetadataTest, releasedOverflowedCopiesAreEvictedFromEitherTier) {
  Metadata metadata(exact());
  registerServer(metadata, "a:1", Tier::kDram, {2});
  registerServer(metadata, "b:1", Tier::kLocalFs, {2});
  const auto job = blockingWait(metadata.co_createBucket());
  // In DRAM, going on in local FS.
  Replica copy{.tier = Tier::kDram};
  for (auto tier : {Tier::kDram, Tier::kLocalFs, Tier::kLocalFs}) {
    copy.blocks.push_back(blockingWait(metadata.co_allocate(tier, 1))[0]);
  }
  const auto chunk = blockingWait(metadata.co_commit(ChunkInfo{
                                      .id = ChunkId{1, 1},
                                      .bucket = job,
                                      .size = 3 * kBlockSize,
                                      .replicas = {copy}}))
                         .id;
  EXPECT_TRUE(blockingWait(metadata.co_releaseBucket(job)));
  // Local FS needs room: the whole copy goes, its DRAM block too.
  blockingWait(metadata.co_allocate(Tier::kLocalFs, 1));
  EXPECT_FALSE(exists(metadata, chunk));
  EXPECT_EQ(metadata.evictions(), 1);
  EXPECT_EQ(metadata.freeBlocks(Tier::kDram), 2);
  EXPECT_EQ(metadata.freeBlocks(Tier::kLocalFs), 1);
}

TEST(MetadataTest, journalKeepsReleasedBuckets) {
  folly::test::TemporaryDirectory dir;
  MetadataOptions options = exact();
  options.journal = dir.path().string() + "/journal";
  std::vector<RegionInfo> regions;
  BucketId job;
  ChunkId output;
  {
    Metadata metadata(options);
    registerServer(metadata, "a:1", Tier::kDram, {4});
    regions = registerServer(metadata, "b:1", Tier::kLocalFs, {4});
    job = blockingWait(metadata.co_createBucket());
    output = blockingWait(
                 metadata.co_commit(ChunkInfo{
                     .id = ChunkId{1, 1},
                     .bucket = job,
                     .size = kBlockSize,
                     .placement =
                         {.tier = Tier::kDram, .writeThrough = Tier::kLocalFs},
                     .replicas = {
                         Replica{
                             .tier = Tier::kDram,
                             .blocks = blockingWait(
                                 metadata.co_allocate(Tier::kDram, 1))},
                         Replica{
                             .tier = Tier::kLocalFs,
                             .blocks = blockingWait(
                                 metadata.co_allocate(Tier::kLocalFs, 1))}}}))
                 .id;
    EXPECT_TRUE(blockingWait(metadata.co_releaseBucket(job)));
  }
  // Replayed from the appended records, then from the rewritten journal.
  for (int restart = 0; restart < 2; ++restart) {
    Metadata metadata(options);
    blockingWait(metadata.co_registerServer(regions, 0));
    EXPECT_EQ(
        blockingWait(metadata.co_lookup(output)).placement.retention,
        Retention::kEphemeral);
    // Later commits into the bucket are ephemeral too.
    registerServer(metadata, "a:2", Tier::kDram, {4});
    const auto late = commit(
        metadata, 2 + restart, 1, Tier::kDram, job, Retention::kRetained);
    EXPECT_EQ(
        blockingWait(metadata.co_lookup(late)).placement.retention,
        Retention::kEphemeral);
  }
}

TEST(MetadataTest, journalKeepsRetention) {
  folly::test::TemporaryDirectory dir;
  MetadataOptions options = exact();
  options.journal = dir.path().string() + "/journal";
  std::vector<RegionInfo> regions;
  auto writeThrough = [&](Metadata& metadata, uint64_t id, Retention retention) {
    ChunkInfo chunk{
        .id = ChunkId{id, id},
        .size = kBlockSize,
        .placement =
            {.tier = Tier::kDram,
             .writeThrough = Tier::kLocalFs,
             .retention = retention},
        .replicas = {
            Replica{
                .tier = Tier::kDram,
                .blocks = blockingWait(metadata.co_allocate(Tier::kDram, 1))},
            Replica{
                .tier = Tier::kLocalFs,
                .blocks =
                    blockingWait(metadata.co_allocate(Tier::kLocalFs, 1))}}};
    return blockingWait(metadata.co_commit(std::move(chunk))).id;
  };
  ChunkId kept;
  ChunkId cached;
  {
    Metadata metadata(options);
    registerServer(metadata, "a:1", Tier::kDram, {4});
    regions = registerServer(metadata, "b:1", Tier::kLocalFs, {4});
    kept = writeThrough(metadata, 1, Retention::kRetained);
    cached = writeThrough(metadata, 2, Retention::kEphemeral);
  }

  Metadata metadata(options);
  blockingWait(metadata.co_registerServer(regions, 0));
  EXPECT_EQ(
      blockingWait(metadata.co_lookup(kept)).placement.retention,
      Retention::kRetained);
  EXPECT_EQ(
      blockingWait(metadata.co_lookup(cached)).placement.retention,
      Retention::kEphemeral);
  // Filling the local FS evicts the ephemeral chunk's only copy, not the
  // retained one's.
  blockingWait(metadata.co_allocate(Tier::kLocalFs, 3));
  EXPECT_FALSE(exists(metadata, cached));
  EXPECT_EQ(
      errorOf([&]() -> folly::coro::Task<void> {
        co_await metadata.co_allocate(Tier::kLocalFs, 1);
      }()),
      ErrorCode::kOutOfSpace);
  EXPECT_EQ(tiersOf(metadata, kept), std::vector{Tier::kLocalFs});
}

TEST(MetadataTest, bucketsRemoveTheirChunks) {
  Metadata metadata(exact());
  registerServer(metadata, "a:1", Tier::kDram, {8});
  const auto job = blockingWait(metadata.co_createBucket());
  EXPECT_NE(job, kDefaultBucket);
  EXPECT_NE(blockingWait(metadata.co_createBucket()), job);
  const auto a = commit(metadata, 1, 2, Tier::kDram, job);
  const auto b = commit(metadata, 2, 2, Tier::kDram, job);
  const auto outside = commit(metadata, 3, 2);
  EXPECT_EQ(blockingWait(metadata.co_lookup(a)).bucket, job);
  EXPECT_EQ(
      errorOf([&]() -> folly::coro::Task<void> {
        co_await metadata.co_deleteBucket(kDefaultBucket);
      }()),
      ErrorCode::kInvalidArgument);

  EXPECT_TRUE(blockingWait(metadata.co_deleteBucket(job)));
  EXPECT_FALSE(blockingWait(metadata.co_deleteBucket(job)));
  EXPECT_FALSE(exists(metadata, a));
  EXPECT_FALSE(exists(metadata, b));
  EXPECT_TRUE(exists(metadata, outside));
  EXPECT_EQ(metadata.freeBlocks(Tier::kDram), 6);

  // A late commit into the deleted bucket fails and frees its blocks.
  EXPECT_EQ(
      errorOf([&]() -> folly::coro::Task<void> {
        commit(metadata, 4, 2, Tier::kDram, job);
        co_return;
      }()),
      ErrorCode::kNotFound);
  EXPECT_EQ(metadata.freeBlocks(Tier::kDram), 6);
  EXPECT_FALSE(exists(metadata, ChunkId{4, 4}));
}

TEST(MetadataTest, deletedBucketsSkipTheGracePeriod) {
  Metadata metadata(MetadataOptions{
      .gracePeriod = 30s, .evictBelow = 0, .evictUntil = 0});
  registerServer(metadata, "a:1", Tier::kDram, {8});
  // Removed: its blocks wait out the 30 s.
  EXPECT_TRUE(blockingWait(metadata.co_remove(commit(metadata, 1, 2))));
  const auto now = blockingWait(metadata.co_createBucket());
  commit(metadata, 2, 2, Tier::kDram, now);
  const auto soon = blockingWait(metadata.co_createBucket());
  commit(metadata, 3, 2, Tier::kDram, soon);
  EXPECT_EQ(metadata.freeBlocks(Tier::kDram), 2);

  // By default, at once.
  blockingWait(metadata.co_deleteBucket(now));
  EXPECT_EQ(metadata.freeBlocks(Tier::kDram), 4);
  EXPECT_EQ(metadata.graceBlocks(Tier::kDram), 2);

  // A short grace: reusable before the removed chunk's blocks.
  blockingWait(metadata.co_deleteBucket(soon, 20ms));
  EXPECT_EQ(metadata.graceBlocks(Tier::kDram), 4);
  const auto start = std::chrono::steady_clock::now();
  EXPECT_EQ(blockingWait(metadata.co_allocate(Tier::kDram, 6)).size(), 6);
  EXPECT_LT(std::chrono::steady_clock::now() - start, 5s);
  EXPECT_EQ(metadata.graceBlocks(Tier::kDram), 2);
}

TEST(MetadataTest, evictedChunksLeaveTheirBucket) {
  Metadata metadata(exact());
  registerServer(metadata, "a:1", Tier::kDram, {4});
  const auto job = blockingWait(metadata.co_createBucket());
  const auto evicted = commit(metadata, 1, 4, Tier::kDram, job);
  const auto kept = commit(metadata, 2, 4, Tier::kDram, job);
  EXPECT_FALSE(exists(metadata, evicted));
  EXPECT_TRUE(blockingWait(metadata.co_deleteBucket(job)));
  EXPECT_FALSE(exists(metadata, kept));
  EXPECT_EQ(metadata.freeBlocks(Tier::kDram), 4);
}

TEST(MetadataTest, bucketsSurviveRestartWithJournal) {
  folly::test::TemporaryDirectory dir;
  MetadataOptions options = exact();
  options.journal = dir.path().string() + "/journal";
  BucketId job;
  BucketId gone;
  ChunkId chunk;
  {
    Metadata metadata(options);
    registerServer(metadata, "a:1", Tier::kDram, {4});
    job = blockingWait(metadata.co_createBucket());
    gone = blockingWait(metadata.co_createBucket());
    blockingWait(metadata.co_deleteBucket(gone));
    ChunkInfo info{
        .id = ChunkId{1, 1},
        .bucket = job,
        .size = kBlockSize,
        .placement = {.tier = Tier::kDram, .writeThrough = Tier::kObjectStore},
        .replicas = {
            Replica{
                .tier = Tier::kDram,
                .blocks = blockingWait(metadata.co_allocate(Tier::kDram, 1))},
            Replica{.tier = Tier::kObjectStore}}};
    chunk = blockingWait(metadata.co_commit(std::move(info))).id;
  }
  {
    Metadata metadata(options);
    EXPECT_TRUE(exists(metadata, chunk));
    EXPECT_FALSE(blockingWait(metadata.co_deleteBucket(gone)));
    // The ids of removed buckets are not handed out again.
    EXPECT_GT(blockingWait(metadata.co_createBucket()), gone);
    EXPECT_TRUE(blockingWait(metadata.co_deleteBucket(job)));
  }
  Metadata metadata(options);
  EXPECT_FALSE(exists(metadata, chunk));
  EXPECT_FALSE(blockingWait(metadata.co_deleteBucket(job)));
  EXPECT_GT(blockingWait(metadata.co_createBucket()), gone + 1);
}

std::optional<Ref> cas(
    Metadata& metadata,
    const std::string& name,
    std::optional<uint64_t> expectedVersion,
    const std::string& value) {
  return blockingWait(metadata.co_casRef(name, expectedVersion, value));
}

TEST(MetadataTest, refsCompareAndSwap) {
  Metadata metadata;
  EXPECT_EQ(blockingWait(metadata.co_getRef("a")), std::nullopt);

  auto a = cas(metadata, "a", std::nullopt, "one");
  ASSERT_TRUE(a);
  EXPECT_EQ(a->value, "one");
  EXPECT_EQ(blockingWait(metadata.co_getRef("a")), a);
  // Only if absent.
  EXPECT_EQ(cas(metadata, "a", std::nullopt, "two"), std::nullopt);
  // Only at the expected version.
  EXPECT_EQ(cas(metadata, "a", a->version + 1, "two"), std::nullopt);
  auto updated = cas(metadata, "a", a->version, "two");
  ASSERT_TRUE(updated);
  EXPECT_GT(updated->version, a->version);
  EXPECT_EQ(blockingWait(metadata.co_getRef("a"))->value, "two");

  EXPECT_FALSE(blockingWait(metadata.co_removeRef("a", a->version)));
  EXPECT_TRUE(blockingWait(metadata.co_removeRef("a", updated->version)));
  EXPECT_FALSE(blockingWait(metadata.co_removeRef("a", std::nullopt)));
  // A recreated ref has a new version.
  auto recreated = cas(metadata, "a", std::nullopt, "one");
  EXPECT_GT(recreated->version, updated->version);
  EXPECT_TRUE(blockingWait(metadata.co_removeRef("a", std::nullopt)));
}

TEST(MetadataTest, listRefs) {
  Metadata metadata;
  for (const auto* name : {"memo/b", "memo/a", "memos", "head"}) {
    cas(metadata, name, std::nullopt, name);
  }
  auto names = [&](const std::string& prefix) {
    std::vector<std::string> result;
    for (const auto& ref : blockingWait(metadata.co_listRefs(prefix, "", 0))) {
      result.push_back(ref.name);
    }
    return result;
  };
  using Names = std::vector<std::string>;
  EXPECT_EQ(names("memo/"), (Names{"memo/a", "memo/b"}));
  EXPECT_EQ(names("memo"), (Names{"memo/a", "memo/b", "memos"}));
  EXPECT_EQ(names(""), (Names{"head", "memo/a", "memo/b", "memos"}));
  EXPECT_EQ(names("x"), Names{});

  auto page = [&](const std::string& prefix,
                  const std::string& startAfter,
                  uint32_t limit) {
    std::vector<std::string> result;
    for (const auto& ref :
         blockingWait(metadata.co_listRefs(prefix, startAfter, limit))) {
      result.push_back(ref.name);
    }
    return result;
  };
  EXPECT_EQ(page("", "", 2), (Names{"head", "memo/a"}));
  EXPECT_EQ(page("", "memo/a", 2), (Names{"memo/b", "memos"}));
  EXPECT_EQ(page("", "memos", 2), Names{});
  EXPECT_EQ(page("memo", "a", 1), (Names{"memo/a"}));
  EXPECT_EQ(page("memo", "memo/a", 0), (Names{"memo/b", "memos"}));
  EXPECT_EQ(page("memo/", "z", 0), Names{});
}

TEST(MetadataTest, refLimits) {
  Metadata metadata;
  auto casError = [&](std::string name, std::string value) {
    return errorOf([&]() -> folly::coro::Task<void> {
      co_await metadata.co_casRef(name, std::nullopt, value);
    }());
  };
  EXPECT_EQ(casError("", "v"), ErrorCode::kInvalidArgument);
  EXPECT_EQ(
      casError(std::string(kMaxRefNameSize + 1, 'n'), "v"),
      ErrorCode::kInvalidArgument);
  EXPECT_EQ(
      casError("n", std::string(kMaxRefValueSize + 1, 'v')),
      ErrorCode::kInvalidArgument);
  EXPECT_TRUE(cas(
      metadata,
      std::string(kMaxRefNameSize, 'n'),
      std::nullopt,
      std::string(kMaxRefValueSize, 'v')));
}

TEST(MetadataTest, refsSurviveRestartWithJournal) {
  folly::test::TemporaryDirectory dir;
  MetadataOptions options;
  options.journal = dir.path().string() + "/journal";
  uint64_t lastVersion = 0;
  {
    Metadata metadata(options);
    auto a = cas(metadata, "a", std::nullopt, "one");
    cas(metadata, "a", a->version, "two");
    cas(metadata, "b", std::nullopt, "b");
    auto c = cas(metadata, "c", std::nullopt, "c");
    blockingWait(metadata.co_removeRef("b", std::nullopt));
    blockingWait(metadata.co_removeRef("c", std::nullopt));
    lastVersion = c->version;
  }
  for (int restart = 0; restart < 2; ++restart) {
    Metadata metadata(options);
    auto refs = blockingWait(metadata.co_listRefs("", "", 0));
    ASSERT_EQ(refs.size(), 1);
    EXPECT_EQ(refs[0].name, "a");
    EXPECT_EQ(refs[0].value, "two");
  }
  // Versions of removed refs are not handed out again.
  Metadata metadata(options);
  EXPECT_GT(cas(metadata, "c", std::nullopt, "c")->version, lastVersion);
}

} // namespace
} // namespace kiln::meta
