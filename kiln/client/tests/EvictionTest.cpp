// Eviction as clients see it: writing more than a tier's budget evicts its
// least recently used ephemeral chunks, which then read as not found, and
// moves retained ones from DRAM to local FS.

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
    auto writer = blockingWait(client_.co_create(
        kDefaultBucket, Placement{.retention = Retention::kEphemeral}));
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
    auto writer = blockingWait(client.co_create(kDefaultBucket, placement, size));
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
          kDefaultBucket,
          Placement{.tier = Tier::kDram, .writeThrough = Tier::kLocalFs,
                    .overflow = Tier::kLocalFs})),
      Error);
}

// One DRAM and one local-FS server that may hold 8 MiB each; over shared
// memory or TCP.
class RetentionTest : public testing::TestWithParam<bool> {
 protected:
  RetentionTest()
      : cluster_{local::LocalClusterOptions{
            .dramServers = 1,
            .localFsServers = 1,
            .regionSize = 16 << 20,
            .budget = 8 << 20,
            .metadata = meta::MetadataOptions{
                .gracePeriod = 100ms, .evictBelow = 0, .evictUntil = 0}}},
        client_{ClientOptions{
            .metadata = cluster_.metadataEndpoint(),
            .useShm = GetParam(),
            .chunkCacheTtl = 50ms}} {}

  // Writes 'data' in appends of 1 MiB.
  ChunkId put(const std::string& data, Placement placement = {}) {
    auto writer = blockingWait(client_.co_create(kDefaultBucket, placement));
    auto buffer = client_.allocateBuffer(1 << 20);
    for (size_t done = 0; done < data.size(); done += 1 << 20) {
      const auto n = std::min<size_t>(1 << 20, data.size() - done);
      std::memcpy(buffer.data(), data.data() + done, n);
      buffer.setSize(n);
      buffer = blockingWait(writer->co_append(std::move(buffer)));
    }
    return blockingWait(writer->co_commit());
  }

  std::string get(ChunkId id) {
    const auto size = blockingWait(client_.co_stat(id)).size;
    auto buffer = blockingWait(
        client_.co_read(id, 0, size, client_.allocateBuffer(size)));
    return std::string(
        reinterpret_cast<const char*>(buffer.data()), buffer.size());
  }

  // The tier of each block of the chunk's copies, in order.
  std::vector<Tier> blockTiers(ChunkId id) {
    std::vector<Tier> tiers;
    for (const auto& replica : blockingWait(client_.co_stat(id)).replicas) {
      for (const auto& block : replica.blocks) {
        auto regions =
            blockingWait(cluster_.metadata().co_getRegions({block.region}));
        tiers.push_back(regions[0].tier);
      }
    }
    return tiers;
  }

  // Distinct bytes, so that misplaced blocks show.
  static std::string bytes(size_t size, char seed) {
    std::string data(size, '\0');
    for (size_t i = 0; i < size; ++i) {
      data[i] = static_cast<char>(seed + i * 7 + i / 4093);
    }
    return data;
  }

  local::LocalCluster cluster_;
  Client client_;
};

TEST_P(RetentionTest, retainedChunksOverflowPerAllocation) {
  const Placement placement{.tier = Tier::kDram, .overflow = Tier::kLocalFs};
  const auto a = bytes(3 << 20, 'a');
  const auto b = bytes(8 << 20, 'b');
  const auto idA = put(a, placement);
  // B fills the rest of DRAM and goes on in the local FS.
  const auto idB = put(b, placement);
  EXPECT_EQ(cluster_.metadata().evictions(), 0);
  EXPECT_EQ(blockTiers(idA), std::vector<Tier>(3, Tier::kDram));
  std::vector<Tier> tiers(5, Tier::kDram);
  tiers.resize(8, Tier::kLocalFs);
  EXPECT_EQ(blockTiers(idB), tiers);
  const auto info = blockingWait(client_.co_stat(idB));
  EXPECT_EQ(info.placement.tier, Tier::kDram);
  EXPECT_EQ(info.replicas[0].tier, Tier::kDram);
  EXPECT_TRUE(get(idA) == a);
  EXPECT_TRUE(get(idB) == b);

  // Once DRAM is full, new retained chunks start in the local FS.
  const auto c = bytes(2 << 20, 'c');
  const auto idC = put(c, placement);
  EXPECT_EQ(blockTiers(idC), std::vector<Tier>(2, Tier::kLocalFs));
  EXPECT_TRUE(get(idC) == c);
  EXPECT_EQ(cluster_.metadata().evictions(), 0);
}

TEST_P(RetentionTest, ephemeralChunksOverflowWhole) {
  const Placement placement{
      .tier = Tier::kDram,
      .overflow = Tier::kLocalFs,
      .retention = Retention::kEphemeral};
  put(bytes(3 << 20, 'a'), placement);
  // B starts in DRAM and stays there, evicting A rather than overflowing.
  const auto b = bytes(8 << 20, 'b');
  const auto idB = put(b, placement);
  EXPECT_EQ(blockTiers(idB), std::vector<Tier>(8, Tier::kDram));
  EXPECT_EQ(cluster_.metadata().evictions(), 1);
  EXPECT_TRUE(get(idB) == b);
}

TEST_P(RetentionTest, retainedChunksWithoutOverflowRunOutOfSpace) {
  const auto a = bytes(3 << 20, 'a');
  const auto b = bytes(3 << 20, 'b');
  const auto idA = put(a);
  const auto idB = put(b);
  try {
    put(bytes(3 << 20, 'c'));
    FAIL() << "Evicted a retained chunk";
  } catch (const Error& e) {
    EXPECT_EQ(e.code(), ErrorCode::kOutOfSpace);
  }
  EXPECT_EQ(cluster_.metadata().evictions(), 0);
  std::this_thread::sleep_for(100ms); // Past the client's cache TTL.
  EXPECT_TRUE(get(idA) == a);
  EXPECT_TRUE(get(idB) == b);
}

TEST_P(RetentionTest, writtenThroughChunksLoseTheirDramCopy) {
  const auto a = bytes(3 << 20, 'a');
  const auto idA = put(
      a, Placement{.tier = Tier::kDram, .writeThrough = Tier::kLocalFs});
  put(bytes(3 << 20, 'b'));
  put(bytes(3 << 20, 'c'));
  // A has a copy in local FS too: its DRAM copy is evicted.
  EXPECT_EQ(cluster_.metadata().evictions(), 1);
  std::this_thread::sleep_for(100ms);
  EXPECT_EQ(blockTiers(idA), std::vector<Tier>(3, Tier::kLocalFs));
  EXPECT_TRUE(get(idA) == a);
}

TEST_P(RetentionTest, releasedBucketsAreEvicted) {
  // A job's retained output fills DRAM.
  const auto job = blockingWait(client_.co_createBucket());
  auto putIn = [&](BucketId bucket, const std::string& data) {
    auto writer = blockingWait(client_.co_create(bucket));
    auto buffer = client_.allocateBuffer(data.size());
    std::memcpy(buffer.data(), data.data(), data.size());
    buffer.setSize(data.size());
    blockingWait(writer->co_append(std::move(buffer)));
    return blockingWait(writer->co_commit());
  };
  const auto a = putIn(job, bytes(4 << 20, 'a'));
  const auto b = putIn(job, bytes(4 << 20, 'b'));
  EXPECT_THROW(put(bytes(1 << 20, 'c')), Error);

  // Kept after the job, but evictable: the next write evicts A, the least
  // recently used.
  EXPECT_TRUE(blockingWait(client_.co_releaseBucket(job)));
  EXPECT_EQ(
      blockingWait(cluster_.metadata().co_lookup(b)).placement.retention,
      Retention::kEphemeral);
  const auto c = bytes(1 << 20, 'c');
  const auto idC = put(c);
  EXPECT_EQ(cluster_.metadata().evictions(), 1);
  EXPECT_TRUE(get(idC) == c);
  std::this_thread::sleep_for(100ms); // Past the client's cache TTL.
  EXPECT_FALSE(blockingWait(cluster_.metadata().co_remove(a)));
  EXPECT_TRUE(get(b) == bytes(4 << 20, 'b'));
}

INSTANTIATE_TEST_SUITE_P(
    Transports,
    RetentionTest,
    testing::Bool(),
    [](const testing::TestParamInfo<bool>& info) {
      return info.param ? "shm" : "tcp";
    });

} // namespace
} // namespace kiln::client
