#include <gtest/gtest.h>

#include <folly/coro/BlockingWait.h>

#include "kiln/catalog/Catalog.h"
#include "kiln/client/Client.h"
#include "kiln/common/Error.h"
#include "kiln/local/LocalCluster.h"

namespace kiln::catalog {
namespace {

using folly::coro::blockingWait;

class CatalogTest : public testing::Test {
 protected:
  CatalogTest()
      : cluster_{local::LocalClusterOptions{
            .dramServers = 1,
            .localFsServers = 1,
            .regionSize = 16 << 20,
            .metadata = meta::MetadataOptions{
                .gracePeriod = std::chrono::milliseconds(0)}}},
        client_{client::ClientOptions{
            .metadata = cluster_.metadataEndpoint(),
            .chunkCacheTtl = std::chrono::milliseconds(0)}},
        catalog_{client_} {}

  ChunkId put(const std::string& data, Placement placement = {}) {
    auto writer = blockingWait(client_.co_create(placement));
    auto buffer = client_.allocateBuffer(data.size());
    std::memcpy(buffer.data(), data.data(), data.size());
    buffer.setSize(data.size());
    blockingWait(writer->co_append(std::move(buffer)));
    return blockingWait(writer->co_commit());
  }

  Manifest manifestOf(std::vector<ChunkRange> chunks) {
    return Manifest{
        .chunks = std::move(chunks),
        .schema = "a BIGINT, b VARCHAR",
        .rows = 10,
        .bytes = 100,
        .properties = {{"query", "select 1"}, {"sorted", "a"}}};
  }

  local::LocalCluster cluster_;
  client::Client client_;
  Catalog catalog_;
};

TEST_F(CatalogTest, encodeDecode) {
  const auto manifest = manifestOf(
      {ChunkRange{.id = ChunkId{1, 2}},
       ChunkRange{.id = ChunkId{3, 4}, .offset = 5, .length = 6}});
  const std::string key("binary\0key", 10);
  const auto data = encodeManifest(key, manifest);
  auto [decodedKey, decoded] = decodeManifest(folly::StringPiece(data));
  EXPECT_EQ(decodedKey, key);
  EXPECT_EQ(decoded, manifest);

  for (size_t size : {size_t{0}, size_t{3}, data.size() - 1}) {
    EXPECT_THROW(
        decodeManifest(folly::StringPiece(data.data(), size)), Error);
  }
  EXPECT_THROW(decodeManifest(folly::StringPiece(data + "x")), Error);
  auto otherVersion = data;
  otherVersion[4] = 9;
  EXPECT_THROW(decodeManifest(folly::StringPiece(otherVersion)), Error);
}

TEST_F(CatalogTest, putGetRemove) {
  const auto id = put("some result");
  const auto manifest = manifestOf({ChunkRange{.id = id}});
  EXPECT_EQ(blockingWait(catalog_.co_getMemo("q1")), std::nullopt);
  EXPECT_TRUE(blockingWait(catalog_.co_putMemo("q1", manifest)));
  EXPECT_EQ(blockingWait(catalog_.co_getMemo("q1")), manifest);

  // Create once: the first memo stays.
  EXPECT_FALSE(blockingWait(catalog_.co_putMemo("q1", manifestOf({}))));
  EXPECT_EQ(blockingWait(catalog_.co_getMemo("q1")), manifest);

  EXPECT_TRUE(blockingWait(catalog_.co_removeMemo("q1")));
  EXPECT_FALSE(blockingWait(catalog_.co_removeMemo("q1")));
  EXPECT_EQ(blockingWait(catalog_.co_getMemo("q1")), std::nullopt);
  EXPECT_TRUE(blockingWait(catalog_.co_putMemo("q1", manifest)));
}

TEST_F(CatalogTest, keysOfAnySize) {
  const auto manifest = manifestOf({ChunkRange{.id = put("data")}});
  const std::string longKey(10000, 'k');
  EXPECT_TRUE(blockingWait(catalog_.co_putMemo(longKey, manifest)));
  EXPECT_TRUE(blockingWait(catalog_.co_putMemo("", manifest)));
  EXPECT_EQ(blockingWait(catalog_.co_getMemo(longKey)), manifest);
  EXPECT_EQ(blockingWait(catalog_.co_getMemo("")), manifest);
  EXPECT_EQ(blockingWait(catalog_.co_getMemo(longKey + "x")), std::nullopt);
}

TEST_F(CatalogTest, memosOfLostChunksGo) {
  const auto kept = put("kept");
  const auto lost = put("lost");
  EXPECT_TRUE(blockingWait(catalog_.co_putMemo(
      "q", manifestOf({ChunkRange{.id = kept}, ChunkRange{.id = lost}}))));
  blockingWait(client_.co_remove(lost));

  EXPECT_EQ(blockingWait(catalog_.co_getMemo("q")), std::nullopt);
  // The memo is gone, so the recomputed result can be put.
  EXPECT_EQ(blockingWait(client_.co_getRef(catalog_.refName("q"))), std::nullopt);
  const auto manifest = manifestOf({ChunkRange{.id = put("recomputed")}});
  EXPECT_TRUE(blockingWait(catalog_.co_putMemo("q", manifest)));
  EXPECT_EQ(blockingWait(catalog_.co_getMemo("q")), manifest);
}

TEST_F(CatalogTest, rangesMustFitTheirChunks) {
  const auto id = put("0123456789");
  EXPECT_TRUE(blockingWait(catalog_.co_putMemo(
      "fits", manifestOf({ChunkRange{.id = id, .offset = 4, .length = 6}}))));
  EXPECT_TRUE(blockingWait(catalog_.co_putMemo(
      "too long",
      manifestOf({ChunkRange{.id = id, .offset = 4, .length = 7}}))));
  EXPECT_TRUE(blockingWait(catalog_.co_getMemo("fits")));
  EXPECT_EQ(blockingWait(catalog_.co_getMemo("too long")), std::nullopt);
}

TEST_F(CatalogTest, manifestsAreWrittenThrough) {
  EXPECT_TRUE(blockingWait(
      catalog_.co_putMemo("q", manifestOf({ChunkRange{.id = put("x")}}))));
  const auto ref = blockingWait(client_.co_getRef(catalog_.refName("q")));
  ASSERT_TRUE(ref);
  const auto chunk = blockingWait(client_.co_stat(ChunkId::parse(ref->value)));
  ASSERT_EQ(chunk.replicas.size(), 2);
  EXPECT_EQ(chunk.replicas[1].tier, Tier::kLocalFs);
}

} // namespace
} // namespace kiln::catalog
