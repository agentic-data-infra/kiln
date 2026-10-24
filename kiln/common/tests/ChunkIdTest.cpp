#include <gtest/gtest.h>

#include <unordered_set>

#include <folly/io/IOBuf.h>

#include "kiln/common/ChunkId.h"
#include "kiln/common/Error.h"

namespace kiln {
namespace {

TEST(ChunkIdTest, knownValue) {
  // XXH3_128bits of no bytes, from xxHash's test vectors.
  EXPECT_EQ(
      ChunkId::of(folly::ByteRange{}).toString(),
      "99aa06d3014798d86001c324468d497f");
}

TEST(ChunkIdTest, sameContentSameId) {
  const std::string data(10'000, 'k');
  EXPECT_EQ(
      ChunkId::of(folly::StringPiece{data}),
      ChunkId::of(folly::StringPiece{std::string(data)}));
  EXPECT_NE(
      ChunkId::of(folly::StringPiece{data}),
      ChunkId::of(folly::StringPiece{data}.subpiece(1)));
}

TEST(ChunkIdTest, chainHashesLikeContiguousBytes) {
  const std::string data = "a chunk split over several buffers";
  auto chain = folly::IOBuf::copyBuffer(data.substr(0, 7));
  chain->appendToChain(folly::IOBuf::copyBuffer(data.substr(7, 10)));
  chain->appendToChain(folly::IOBuf::copyBuffer(data.substr(17)));
  ASSERT_TRUE(chain->isChained());
  EXPECT_EQ(ChunkId::of(*chain), ChunkId::of(folly::StringPiece{data}));
}

TEST(ChunkIdTest, hasherMatchesOneShot) {
  const std::string data(3 * 1000 + 7, 'x');
  ChunkHasher hasher;
  for (size_t i = 0; i < data.size(); i += 1000) {
    hasher.update(folly::StringPiece{data}.subpiece(i, 1000));
  }
  EXPECT_EQ(hasher.id(), ChunkId::of(folly::StringPiece{data}));
}

TEST(ChunkIdTest, parse) {
  const auto id = ChunkId::of(folly::StringPiece{"parse me"});
  EXPECT_EQ(ChunkId::parse(id.toString()), id);
  EXPECT_THROW(ChunkId::parse("abc"), Error);
  EXPECT_THROW(ChunkId::parse(std::string(32, 'g')), Error);
}

TEST(ChunkIdTest, hashable) {
  std::unordered_set<ChunkId> ids;
  for (int i = 0; i < 1000; ++i) {
    ids.insert(ChunkId::of(folly::StringPiece{std::to_string(i)}));
  }
  EXPECT_EQ(ids.size(), 1000);
}

} // namespace
} // namespace kiln
