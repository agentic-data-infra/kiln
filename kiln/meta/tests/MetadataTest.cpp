#include <gtest/gtest.h>

#include <folly/coro/BlockingWait.h>

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

// Commits a chunk of 'blocks' full blocks in 'tier'.
ChunkId commit(Metadata& metadata, uint64_t id, uint32_t blocks, Tier tier = Tier::kDram) {
  ChunkInfo chunk{
      .id = ChunkId{id, id},
      .size = blocks * kBlockSize,
      .blocks = blockingWait(metadata.co_allocate(Placement{.tier = tier}, blocks)),
      .placement = Placement{.tier = tier}};
  return blockingWait(metadata.co_commit(std::move(chunk))).id;
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

  auto blocks = blockingWait(metadata.co_allocate(Placement{}, 6));
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
        co_await metadata.co_allocate(Placement{}, 3);
      }()),
      ErrorCode::kOutOfSpace);
  blockingWait(metadata.co_free(blocks));
  EXPECT_EQ(metadata.freeBlocks(Tier::kDram), 8);
  EXPECT_EQ(metadata.graceBlocks(Tier::kDram), 0);
}

TEST(MetadataTest, budgetLimitsAllocation) {
  Metadata metadata(exact());
  // 8 blocks of regions, 3 of them usable.
  registerServer(metadata, "a:1", Tier::kDram, {4, 4}, 3);
  EXPECT_EQ(metadata.freeBlocks(Tier::kDram), 3);
  auto blocks = blockingWait(metadata.co_allocate(Placement{}, 3));
  EXPECT_EQ(metadata.freeBlocks(Tier::kDram), 0);
  EXPECT_EQ(
      errorOf([&]() -> folly::coro::Task<void> {
        co_await metadata.co_allocate(Placement{}, 1);
      }()),
      ErrorCode::kOutOfSpace);
  blockingWait(metadata.co_free(blocks));
  EXPECT_EQ(metadata.freeBlocks(Tier::kDram), 3);
}

TEST(MetadataTest, commitLookupRemove) {
  Metadata metadata(exact());
  registerServer(metadata, "a:1", Tier::kDram, {4});
  auto blocks = blockingWait(metadata.co_allocate(Placement{}, 2));
  blocks[1].length = 10;
  const ChunkInfo chunk{
      .id = ChunkId{1, 2}, .size = kBlockSize + 10, .blocks = blocks};

  EXPECT_EQ(blockingWait(metadata.co_commit(chunk)), chunk);
  EXPECT_EQ(blockingWait(metadata.co_lookup(chunk.id)), chunk);

  // A second commit of the id gets the first chunk; its blocks are free at
  // once, as nobody has seen them.
  auto other = blockingWait(metadata.co_allocate(Placement{}, 2));
  other[1].length = 10;
  auto duplicate = chunk;
  duplicate.blocks = other;
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
  auto blocks = blockingWait(metadata.co_allocate(Placement{}, 1));
  ChunkInfo wrongSize{.id = ChunkId{1, 2}, .size = 5, .blocks = blocks};
  EXPECT_EQ(
      errorOf([&]() -> folly::coro::Task<void> {
        co_await metadata.co_commit(wrongSize);
      }()),
      ErrorCode::kInvalidArgument);
  ChunkInfo noRegion{
      .id = ChunkId{1, 2},
      .size = 5,
      .blocks = {BlockRef{.region = 999, .length = 5}}};
  EXPECT_EQ(
      errorOf([&]() -> folly::coro::Task<void> {
        co_await metadata.co_commit(noRegion);
      }()),
      ErrorCode::kInvalidArgument);
  ChunkInfo wrongTier{
      .id = ChunkId{1, 2},
      .size = kBlockSize,
      .blocks = blocks,
      .placement = Placement{.tier = Tier::kLocalFs}};
  EXPECT_EQ(
      errorOf([&]() -> folly::coro::Task<void> {
        co_await metadata.co_commit(wrongTier);
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
        co_await metadata.co_allocate(Placement{}, 11);
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
  blockingWait(metadata.co_allocate(Placement{}, 3));
  EXPECT_EQ(metadata.evictions(), 3);
  EXPECT_FALSE(exists(metadata, ChunkId{3, 3}));
  EXPECT_TRUE(exists(metadata, ChunkId{4, 4}));
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
  auto blocks = blockingWait(metadata.co_allocate(Placement{}, 2));
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

} // namespace
} // namespace kiln::meta
