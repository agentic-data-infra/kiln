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

  ChunkId put(
      const std::string& data,
      Placement placement = {},
      BucketId bucket = kDefaultBucket) {
    auto writer = blockingWait(client_.co_create(bucket, placement));
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

  Manifest filesOf(
      const std::vector<std::pair<std::string, std::string>>& contents);

  local::LocalCluster cluster_;
  client::Client client_;
  Catalog catalog_;
};

TEST_F(CatalogTest, encodeDecode) {
  auto manifest = manifestOf(
      {ChunkRange{.id = ChunkId{1, 2}},
       ChunkRange{.id = ChunkId{3, 4}, .offset = 5, .length = 6},
       ChunkRange{
           .id = ChunkId{7, 8},
           .length = 9,
           .name = "dir/part-00000",
           .rows = 3},
       ChunkRange{
           .id = ChunkId{9, 10}, .name = "dir", .kind = EntryKind::kManifest}});
  manifest.bucket = 11;
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

// Version 1, as memos stored before entries had names, kinds and rows.
TEST_F(CatalogTest, decodesVersion1) {
  std::string data("KMFT", 4);
  auto u32 = [&](uint32_t value) {
    data.append(reinterpret_cast<const char*>(&value), sizeof(value));
  };
  auto u64 = [&](uint64_t value) {
    data.append(reinterpret_cast<const char*>(&value), sizeof(value));
  };
  auto string = [&](const std::string& value) {
    u64(value.size());
    data.append(value);
  };
  u32(1);
  string("key");
  string("a BIGINT");
  u64(10);
  u64(100);
  u64(2);
  for (const auto& [id, length] :
       {std::pair{ChunkId{1, 2}, std::optional<uint64_t>()},
        std::pair{ChunkId{3, 4}, std::optional<uint64_t>(6)}}) {
    u64(id.high);
    u64(id.low);
    u64(id.high == 3 ? 5 : 0);
    u32(length ? 1 : 0);
    u64(length.value_or(0));
  }
  u64(1);
  string("query");
  string("select 1");

  auto [key, manifest] = decodeManifest(folly::StringPiece(data));
  EXPECT_EQ(key, "key");
  EXPECT_EQ(
      manifest,
      (Manifest{
          .chunks =
              {ChunkRange{.id = ChunkId{1, 2}},
               ChunkRange{.id = ChunkId{3, 4}, .offset = 5, .length = 6}},
          .schema = "a BIGINT",
          .rows = 10,
          .bytes = 100,
          .properties = {{"query", "select 1"}}}));
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

// A manifest of named files of 'contents', in a new bucket.
Manifest CatalogTest::filesOf(
    const std::vector<std::pair<std::string, std::string>>& contents) {
  Manifest manifest{.bucket = blockingWait(client_.co_createBucket())};
  for (const auto& [name, data] : contents) {
    manifest.chunks.push_back(
        {.id = put(data, {}, manifest.bucket),
         .length = data.size(),
         .name = name});
    manifest.bytes += data.size();
  }
  return manifest;
}

TEST_F(CatalogTest, publishGetDelete) {
  EXPECT_EQ(blockingWait(catalog_.co_getDataset("input")), std::nullopt);
  const auto manifest = filesOf({{"part-00000", "abc"}, {"part-00001", "de"}});
  const auto version =
      blockingWait(catalog_.co_publishDataset("input", manifest));
  ASSERT_TRUE(version);

  const auto dataset = blockingWait(catalog_.co_getDataset("input"));
  ASSERT_TRUE(dataset);
  EXPECT_EQ(dataset->name, "input");
  EXPECT_EQ(dataset->version, *version);
  EXPECT_EQ(dataset->manifest, manifest);
  const auto& file = dataset->manifest.chunks[1];
  auto data = blockingWait(client_.co_read(
      file.id, 0, *file.length, client_.allocateBuffer(*file.length)));
  EXPECT_EQ(data.bytes().toString(), "de");

  // Create only without an expected version.
  EXPECT_EQ(
      blockingWait(catalog_.co_publishDataset("input", filesOf({}))),
      std::nullopt);
  EXPECT_EQ(blockingWait(catalog_.co_getDataset("input"))->manifest, manifest);

  EXPECT_TRUE(blockingWait(catalog_.co_deleteDataset("input")));
  EXPECT_FALSE(blockingWait(catalog_.co_deleteDataset("input")));
  EXPECT_EQ(blockingWait(catalog_.co_getDataset("input")), std::nullopt);
  // The files went with the dataset's bucket, the manifest with it.
  EXPECT_THROW(blockingWait(client_.co_stat(file.id)), Error);
  EXPECT_THROW(blockingWait(client_.co_stat(dataset->manifestId)), Error);
}

TEST_F(CatalogTest, newVersions) {
  const auto first = filesOf({{"a", "1"}});
  const auto v1 = blockingWait(catalog_.co_publishDataset("d", first));
  ASSERT_TRUE(v1);
  const auto second = filesOf({{"a", "1"}, {"b", "2"}});
  // Only at the expected version.
  EXPECT_EQ(
      blockingWait(catalog_.co_publishDataset("d", second, *v1 + 1000)),
      std::nullopt);
  const auto v2 = blockingWait(catalog_.co_publishDataset("d", second, *v1));
  ASSERT_TRUE(v2);
  EXPECT_GT(*v2, *v1);
  const auto dataset = blockingWait(catalog_.co_getDataset("d"));
  EXPECT_EQ(dataset->version, *v2);
  EXPECT_EQ(dataset->manifest, second);
  // The first version's chunks stay.
  EXPECT_EQ(blockingWait(client_.co_stat(first.chunks[0].id)).size, 1);
}

TEST_F(CatalogTest, lostPublishRemovesItsManifest) {
  ASSERT_TRUE(blockingWait(catalog_.co_publishDataset("d", filesOf({}))));
  const auto other = filesOf({{"x", "lost"}});
  EXPECT_EQ(
      blockingWait(catalog_.co_publishDataset("d", other)), std::nullopt);
  const auto id = ChunkId::of(folly::StringPiece(encodeManifest("d", other)));
  EXPECT_THROW(blockingWait(client_.co_stat(id)), Error);
}

TEST_F(CatalogTest, listDatasets) {
  for (const auto* name : {"sort-input", "sort-output", "other"}) {
    ASSERT_TRUE(blockingWait(catalog_.co_publishDataset(name, filesOf({}))));
  }
  EXPECT_TRUE(blockingWait(catalog_.co_putMemo("q", manifestOf({}))));
  EXPECT_EQ(
      blockingWait(catalog_.co_listDatasets()),
      (std::vector<std::string>{"other", "sort-input", "sort-output"}));
  EXPECT_EQ(
      blockingWait(catalog_.co_listDatasets("sort-")),
      (std::vector<std::string>{"sort-input", "sort-output"}));
}

TEST_F(CatalogTest, datasetNames) {
  for (const auto& name :
       {std::string(), std::string("a/b"), std::string(kMaxRefNameSize, 'n')}) {
    EXPECT_THROW(
        blockingWait(catalog_.co_publishDataset(name, filesOf({}))), Error);
    EXPECT_THROW(blockingWait(catalog_.co_getDataset(name)), Error);
  }
}

} // namespace
} // namespace kiln::catalog
