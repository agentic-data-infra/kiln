// The client against an in-process cluster, over TCP and over shared memory.

#include <gtest/gtest.h>

#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>

#include "kiln/client/Client.h"
#include "kiln/common/Error.h"
#include "kiln/local/LocalCluster.h"

namespace kiln::client {
namespace {

using folly::coro::blockingWait;

// 'size' bytes of a pattern that differs with 'seed'.
std::string bytes(size_t size, uint32_t seed = 1) {
  std::string data(size, '\0');
  uint32_t x = seed * 2654435761u + 1;
  for (auto& c : data) {
    x = x * 1664525u + 1013904223u;
    c = static_cast<char>(x >> 24);
  }
  return data;
}

Buffer toBuffer(Client& client, const std::string& data) {
  auto buffer = client.allocateBuffer(data.size());
  std::memcpy(buffer.data(), data.data(), data.size());
  buffer.setSize(data.size());
  return buffer;
}

std::string toString(const Buffer& buffer) {
  return std::string(
      reinterpret_cast<const char*>(buffer.data()), buffer.size());
}

class ClientTest : public testing::TestWithParam<bool> {
 protected:
  static void SetUpTestSuite() {
    cluster_ = new local::LocalCluster(local::LocalClusterOptions{
        .dramServers = 2,
        .localFsServers = 2,
        .regionsPerServer = 2,
        .regionSize = 16 << 20,
        // Exact block counts: freed blocks are free at once, and nothing is
        // evicted before it has to be (EvictionTest covers eviction).
        .metadata = meta::MetadataOptions{
            .gracePeriod = std::chrono::milliseconds(0),
            .evictBelow = 0,
            .evictUntil = 0}});
  }

  static void TearDownTestSuite() {
    delete cluster_;
    cluster_ = nullptr;
  }

  void SetUp() override {
    client_ = std::make_unique<Client>(ClientOptions{
        .metadata = cluster_->metadataEndpoint(), .useShm = GetParam()});
  }

  // Writes 'data' as one chunk, appended in pieces of 'piece' bytes.
  ChunkId put(
      const std::string& data,
      Placement placement = {},
      size_t piece = 3 << 20) {
    auto writer = blockingWait(client_->co_create(placement));
    auto buffer = client_->allocateBuffer(piece);
    for (size_t done = 0; done < data.size(); done += piece) {
      const auto n = std::min(piece, data.size() - done);
      std::memcpy(buffer.data(), data.data() + done, n);
      buffer.setSize(n);
      buffer = blockingWait(writer->co_append(std::move(buffer)));
    }
    return blockingWait(writer->co_commit());
  }

  std::string get(ChunkId id, uint64_t offset = 0, uint64_t length = ~0ull) {
    const auto size = blockingWait(client_->co_stat(id)).size;
    auto buffer = client_->allocateBuffer(std::min(length, size));
    buffer = blockingWait(
        client_->co_read(id, offset, length, std::move(buffer)));
    return toString(buffer);
  }

  static local::LocalCluster* cluster_;
  std::unique_ptr<Client> client_;
};

local::LocalCluster* ClientTest::cluster_ = nullptr;

TEST_P(ClientTest, putAndGet) {
  for (auto tier : {Tier::kDram, Tier::kLocalFs}) {
    SCOPED_TRACE(toString(tier));
    for (size_t size : {size_t{0}, size_t{1}, size_t{1000}, kBlockSize,
                        kBlockSize + 1, size_t{10 << 20} + 17}) {
      SCOPED_TRACE(size);
      const auto data = bytes(size, static_cast<uint32_t>(size) + 7);
      const auto id = put(data, Placement{.tier = tier});
      EXPECT_EQ(id, ChunkId::of(folly::StringPiece(data)));
      EXPECT_EQ(get(id), data);

      const auto info = blockingWait(client_->co_stat(id));
      EXPECT_EQ(info.size, size);
      EXPECT_EQ(info.blocks.size(), (size + kBlockSize - 1) / kBlockSize);
      EXPECT_EQ(info.placement.tier, tier);
      EXPECT_TRUE(blockingWait(client_->co_remove(id)));
    }
  }
}

TEST_P(ClientTest, blocksSpreadOverServers) {
  const auto id = put(bytes(8 << 20));
  const auto info = blockingWait(client_->co_stat(id));
  std::set<RegionId> regions;
  for (const auto& block : info.blocks) {
    regions.insert(block.region);
  }
  // Two DRAM servers with two regions each.
  EXPECT_EQ(regions.size(), 4);
  blockingWait(client_->co_remove(id));
}

TEST_P(ClientTest, rangeReads) {
  const auto data = bytes(5 * kBlockSize + 123);
  const auto id = put(data, {}, 1 << 20);
  // Within a block, across block boundaries, to the end and past it.
  for (auto [offset, length] : std::vector<std::pair<uint64_t, uint64_t>>{
           {0, 10},
           {kBlockSize - 5, 10},
           {kBlockSize / 2, 3 * kBlockSize},
           {5 * kBlockSize, 123},
           {5 * kBlockSize + 100, 1000},
           {data.size(), 10}}) {
    SCOPED_TRACE(fmt::format("{}+{}", offset, length));
    EXPECT_EQ(get(id, offset, length), data.substr(offset, length));
  }
  try {
    get(id, data.size() + 1, 1);
    FAIL() << "Read past the end";
  } catch (const Error& e) {
    EXPECT_EQ(e.code(), ErrorCode::kInvalidArgument);
  }
  blockingWait(client_->co_remove(id));
}

TEST_P(ClientTest, readIntoRegisteredMemory) {
  const auto data = bytes(3 << 20);
  const auto id = put(data);
  std::vector<char> memory(data.size());
  auto buffer = client_->registerMemory(memory.data(), memory.size());
  buffer =
      blockingWait(client_->co_read(id, 0, data.size(), std::move(buffer)));
  EXPECT_EQ(buffer.data(), reinterpret_cast<uint8_t*>(memory.data()));
  EXPECT_EQ(std::string(memory.data(), memory.size()), data);
  blockingWait(client_->co_remove(id));
}

TEST_P(ClientTest, firstCommitWins) {
  const auto& metadata = cluster_->metadata();
  const auto freeBefore = metadata.freeBlocks(Tier::kDram);
  const auto data = bytes(3 << 20, 42);
  const auto first = put(data);
  const auto blocks = blockingWait(client_->co_stat(first)).blocks;
  EXPECT_EQ(metadata.freeBlocks(Tier::kDram), freeBefore - 3);

  // A second client writes the same bytes: same id, same blocks, and its
  // own blocks are freed.
  Client other(ClientOptions{
      .metadata = cluster_->metadataEndpoint(), .useShm = GetParam()});
  auto writer = blockingWait(other.co_create());
  blockingWait(writer->co_append(toBuffer(other, data)));
  EXPECT_EQ(blockingWait(writer->co_commit()), first);
  EXPECT_EQ(blockingWait(other.co_stat(first)).blocks, blocks);
  EXPECT_EQ(metadata.freeBlocks(Tier::kDram), freeBefore - 3);

  blockingWait(client_->co_remove(first));
  EXPECT_EQ(metadata.freeBlocks(Tier::kDram), freeBefore);
}

TEST_P(ClientTest, abortFreesBlocks) {
  const auto& metadata = cluster_->metadata();
  const auto freeBefore = metadata.freeBlocks(Tier::kLocalFs);
  auto writer =
      blockingWait(client_->co_create(Placement{.tier = Tier::kLocalFs}));
  blockingWait(writer->co_append(toBuffer(*client_, bytes(2 << 20))));
  EXPECT_EQ(metadata.freeBlocks(Tier::kLocalFs), freeBefore - 2);
  blockingWait(writer->co_abort());
  EXPECT_EQ(metadata.freeBlocks(Tier::kLocalFs), freeBefore);
  EXPECT_THROW(blockingWait(writer->co_commit()), Error);
}

TEST_P(ClientTest, notFound) {
  const auto id = ChunkId::of(folly::StringPiece("never written"));
  try {
    blockingWait(client_->co_stat(id));
    FAIL() << "Found a chunk that was never written";
  } catch (const Error& e) {
    EXPECT_EQ(e.code(), ErrorCode::kNotFound);
  }
  EXPECT_FALSE(blockingWait(client_->co_remove(id)));
}

TEST_P(ClientTest, outOfSpace) {
  // 2 servers * 2 regions * 16 MiB of DRAM.
  auto writer = blockingWait(client_->co_create());
  auto buffer = toBuffer(*client_, bytes(65 << 20));
  try {
    blockingWait(writer->co_append(std::move(buffer)));
    FAIL() << "Wrote more than the tier holds";
  } catch (const Error& e) {
    EXPECT_EQ(e.code(), ErrorCode::kOutOfSpace);
  }
  blockingWait(writer->co_abort());
}

TEST_P(ClientTest, concurrentWriters) {
  std::vector<std::string> data;
  for (uint32_t i = 0; i < 8; ++i) {
    data.push_back(bytes((i + 1) << 19, i));
  }
  auto write = [&](const std::string& bytes) -> folly::coro::Task<ChunkId> {
    auto writer = co_await client_->co_create();
    co_await writer->co_append(toBuffer(*client_, bytes));
    co_return co_await writer->co_commit();
  };
  std::vector<folly::coro::Task<ChunkId>> writes;
  for (const auto& bytes : data) {
    writes.push_back(write(bytes));
  }
  auto ids = blockingWait(folly::coro::collectAllRange(std::move(writes)));
  for (size_t i = 0; i < data.size(); ++i) {
    EXPECT_EQ(get(ids[i]), data[i]);
    blockingWait(client_->co_remove(ids[i]));
  }
}

INSTANTIATE_TEST_SUITE_P(
    Transports,
    ClientTest,
    testing::Values(false, true),
    [](const auto& info) { return info.param ? "shm" : "tcp"; });

} // namespace
} // namespace kiln::client
