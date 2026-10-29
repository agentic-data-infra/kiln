// Eviction as clients see it: writing more than a tier's budget evicts its
// least recently used chunks, which then read as not found.

#include <gtest/gtest.h>

#include <thread>

#include <folly/coro/BlockingWait.h>

#include "kiln/client/Client.h"
#include "kiln/common/Error.h"
#include "kiln/local/LocalCluster.h"

namespace kiln::client {
namespace {

using folly::coro::blockingWait;
using namespace std::chrono_literals;

class EvictionTest : public testing::Test {
 protected:
  // One DRAM server whose 16 MiB region may hold 8 MiB.
  EvictionTest()
      : cluster_{local::LocalClusterOptions{
            .dramServers = 1,
            .localFsServers = 0,
            .regionSize = 16 << 20,
            .budget = 8 << 20,
            .metadata = meta::MetadataOptions{
                .gracePeriod = 200ms, .evictBelow = 0, .evictUntil = 0}}},
        client_{ClientOptions{
            .metadata = cluster_.metadataEndpoint(),
            .chunkCacheTtl = 50ms}} {}

  ChunkId put(char fill, size_t size) {
    auto writer = blockingWait(client_.co_create());
    auto buffer = client_.allocateBuffer(size);
    std::memset(buffer.data(), fill, size);
    buffer.setSize(size);
    blockingWait(writer->co_append(std::move(buffer)));
    return blockingWait(writer->co_commit());
  }

  std::optional<std::string> get(ChunkId id) {
    try {
      const auto size = blockingWait(client_.co_stat(id)).size;
      auto buffer = blockingWait(
          client_.co_read(id, 0, size, client_.allocateBuffer(size)));
      return std::string(
          reinterpret_cast<const char*>(buffer.data()), buffer.size());
    } catch (const Error& e) {
      EXPECT_EQ(e.code(), ErrorCode::kNotFound);
      return std::nullopt;
    }
  }

  local::LocalCluster cluster_;
  Client client_;
};

TEST_F(EvictionTest, oldestChunksGo) {
  // Five chunks of 3 MiB in 8 MiB: each new one evicts the oldest.
  std::vector<ChunkId> ids;
  for (char fill : {'a', 'b', 'c', 'd', 'e'}) {
    ids.push_back(put(fill, 3 << 20));
  }
  EXPECT_EQ(cluster_.metadata().evictions(), 3);

  std::this_thread::sleep_for(100ms); // Past the client's cache TTL.
  EXPECT_FALSE(get(ids[0]));
  EXPECT_FALSE(get(ids[1]));
  EXPECT_FALSE(get(ids[2]));
  EXPECT_TRUE(get(ids[3]) == std::string(3 << 20, 'd'));
  EXPECT_TRUE(get(ids[4]) == std::string(3 << 20, 'e'));
}

TEST_F(EvictionTest, readsKeepChunksAlive) {
  const auto a = put('a', 3 << 20);
  const auto b = put('b', 3 << 20);
  // Reading a after the cache TTL tells the metadata server it is in use.
  std::this_thread::sleep_for(100ms);
  EXPECT_TRUE(get(a));
  put('c', 3 << 20);

  std::this_thread::sleep_for(100ms);
  EXPECT_TRUE(get(a));
  EXPECT_FALSE(get(b));
}

TEST_F(EvictionTest, cachedReadersFinishWithinTheGracePeriod) {
  const auto a = put('a', 3 << 20);
  put('b', 3 << 20);
  // A reader that knows a's blocks.
  Client reader(ClientOptions{.metadata = cluster_.metadataEndpoint()});
  EXPECT_EQ(blockingWait(reader.co_stat(a)).size, 3 << 20);

  // The next chunk evicts a, then waits out the grace period of a's blocks
  // before writing into them; meanwhile the reader still reads a.
  std::thread writer([&] { put('c', 3 << 20); });
  std::this_thread::sleep_for(50ms);
  EXPECT_EQ(cluster_.metadata().evictions(), 1);
  auto buffer = blockingWait(
      reader.co_read(a, 0, 3 << 20, reader.allocateBuffer(3 << 20)));
  EXPECT_TRUE(
      std::string(reinterpret_cast<const char*>(buffer.data()), buffer.size()) ==
      std::string(3 << 20, 'a'));
  writer.join();
}

// DRAM first, overflow to local FS: a chunk that does not fit into free
// DRAM goes to local FS as a whole, and no DRAM chunk is evicted for it.
TEST(OverflowTest, wholeChunksOverflowWithoutEvicting) {
  local::LocalCluster cluster(local::LocalClusterOptions{
      .dramServers = 1,
      .localFsServers = 1,
      .regionSize = 8 << 20,
      .metadata = meta::MetadataOptions{.gracePeriod = 0ms}});
  Client client(ClientOptions{.metadata = cluster.metadataEndpoint()});
  const Placement placement{.tier = Tier::kDram, .overflow = Tier::kLocalFs};

  // Four chunks of 3 MiB into 8 MiB of DRAM.
  std::vector<ChunkId> ids;
  for (char fill = 'a'; fill < 'e'; ++fill) {
    const size_t size = 3 << 20;
    auto writer = blockingWait(client.co_create(placement, size));
    auto buffer = client.allocateBuffer(size);
    std::memset(buffer.data(), fill, size);
    buffer.setSize(size);
    blockingWait(writer->co_append(std::move(buffer)));
    ids.push_back(blockingWait(writer->co_commit()));
  }
  std::vector<Tier> tiers;
  for (const auto& id : ids) {
    const auto info = blockingWait(client.co_stat(id));
    ASSERT_EQ(info.replicas.size(), 1);
    EXPECT_EQ(info.placement, Placement{.tier = info.replicas[0].tier});
    tiers.push_back(info.replicas[0].tier);
  }
  EXPECT_EQ(
      tiers,
      (std::vector<Tier>{
          Tier::kDram, Tier::kDram, Tier::kLocalFs, Tier::kLocalFs}));
  EXPECT_EQ(cluster.metadata().evictions(), 0);

  EXPECT_THROW(
      blockingWait(client.co_create(
          Placement{.tier = Tier::kDram, .writeThrough = Tier::kLocalFs,
                    .overflow = Tier::kLocalFs})),
      Error);
}

} // namespace
} // namespace kiln::client
