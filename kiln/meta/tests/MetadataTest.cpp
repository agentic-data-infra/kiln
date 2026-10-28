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

// Commits a chunk of 'blocks' full blocks in 'tier'.
ChunkId commit(Metadata& metadata, uint64_t id, uint32_t blocks, Tier tier = Tier::kDram) {
  ChunkInfo chunk{
      .id = ChunkId{id, id},
      .size = blocks * kBlockSize,
      .placement = Placement{.tier = tier},
      .replicas = {Replica{
          .tier = tier,
          .blocks = blockingWait(metadata.co_allocate(tier, blocks))}}};
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

  // Evicting the DRAM copy leaves the local-FS one.
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
