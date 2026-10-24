#include <gtest/gtest.h>

#include <folly/coro/BlockingWait.h>

#include "kiln/common/Error.h"
#include "kiln/meta/Metadata.h"

namespace kiln::meta {
namespace {

using folly::coro::blockingWait;

RegionInfo region(const std::string& server, Tier tier, uint64_t blocks) {
  return RegionInfo{.server = server, .tier = tier, .length = blocks * kBlockSize};
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

TEST(MetadataTest, registerAssignsIds) {
  Metadata metadata;
  auto regions = blockingWait(metadata.co_registerServer(
      {region("a:1", Tier::kDram, 4), region("a:1", Tier::kLocalFs, 2)}));
  ASSERT_EQ(regions.size(), 2);
  EXPECT_NE(regions[0].id, regions[1].id);
  EXPECT_EQ(blockingWait(metadata.co_getRegions({regions[1].id})), std::vector{regions[1]});
  EXPECT_EQ(metadata.freeBlocks(Tier::kDram), 4);
  EXPECT_EQ(metadata.freeBlocks(Tier::kLocalFs), 2);
  EXPECT_EQ(
      errorOf([&]() -> folly::coro::Task<void> {
        co_await metadata.co_getRegions({12345});
      }()),
      ErrorCode::kNotFound);
}

TEST(MetadataTest, allocateRoundRobin) {
  Metadata metadata;
  blockingWait(metadata.co_registerServer({region("a:1", Tier::kDram, 4)}));
  blockingWait(metadata.co_registerServer({region("b:1", Tier::kDram, 4)}));
  blockingWait(metadata.co_registerServer({region("c:1", Tier::kLocalFs, 4)}));

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

  EXPECT_EQ(
      errorOf([&]() -> folly::coro::Task<void> {
        co_await metadata.co_allocate(Placement{}, 3);
      }()),
      ErrorCode::kOutOfSpace);
  blockingWait(metadata.co_free(blocks));
  EXPECT_EQ(metadata.freeBlocks(Tier::kDram), 8);
}

TEST(MetadataTest, commitLookupRemove) {
  Metadata metadata;
  blockingWait(metadata.co_registerServer({region("a:1", Tier::kDram, 4)}));
  auto blocks = blockingWait(metadata.co_allocate(Placement{}, 2));
  blocks[1].length = 10;
  const ChunkInfo chunk{
      .id = ChunkId{1, 2}, .size = kBlockSize + 10, .blocks = blocks};

  EXPECT_EQ(blockingWait(metadata.co_commit(chunk)), chunk);
  EXPECT_EQ(blockingWait(metadata.co_lookup(chunk.id)), chunk);

  // A second commit of the id gets the first chunk; its blocks are freed.
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
  EXPECT_EQ(
      errorOf([&]() -> folly::coro::Task<void> {
        co_await metadata.co_lookup(chunk.id);
      }()),
      ErrorCode::kNotFound);
}

TEST(MetadataTest, commitChecksBlocks) {
  Metadata metadata;
  blockingWait(metadata.co_registerServer({region("a:1", Tier::kDram, 4)}));
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
}

TEST(MetadataTest, reregisteringDropsOldRegionsAndChunks) {
  Metadata metadata;
  blockingWait(metadata.co_registerServer({region("a:1", Tier::kDram, 4)}));
  auto blocks = blockingWait(metadata.co_allocate(Placement{}, 1));
  blockingWait(metadata.co_commit(
      ChunkInfo{.id = ChunkId{1, 2}, .size = kBlockSize, .blocks = blocks}));
  EXPECT_EQ(metadata.chunkCount(), 1);

  blockingWait(metadata.co_registerServer({region("a:1", Tier::kDram, 2)}));
  EXPECT_EQ(metadata.chunkCount(), 0);
  EXPECT_EQ(metadata.freeBlocks(Tier::kDram), 2);
}

} // namespace
} // namespace kiln::meta
