#include <gtest/gtest.h>

#include <unistd.h>

#include <folly/FileUtil.h>
#include <folly/testing/TestUtil.h>

#include "kiln/common/Error.h"
#include "kiln/meta/Journal.h"

namespace kiln::meta {
namespace {

std::vector<std::string> replay(const std::string& path) {
  std::vector<std::string> records;
  Journal journal(path, [&](folly::ByteRange record) {
    records.emplace_back(reinterpret_cast<const char*>(record.data()), record.size());
  });
  return records;
}

using Records = std::vector<std::string>;

TEST(JournalTest, appendAndReplay) {
  folly::test::TemporaryDirectory dir;
  const auto path = dir.path().string() + "/journal";
  EXPECT_EQ(replay(path), Records{});
  {
    Journal journal(path, [](folly::ByteRange) {});
    journal.append(folly::StringPiece("one"));
    journal.append(folly::StringPiece(""));
    journal.append(folly::StringPiece("three"));
  }
  EXPECT_EQ(replay(path), (Records{"one", "", "three"}));
}

TEST(JournalTest, tornRecordIsCutOff) {
  folly::test::TemporaryDirectory dir;
  const auto path = dir.path().string() + "/journal";
  {
    Journal journal(path, [](folly::ByteRange) {});
    journal.append(folly::StringPiece("one"));
    journal.append(folly::StringPiece("two"));
  }
  std::string data;
  ASSERT_TRUE(folly::readFile(path.c_str(), data));
  ASSERT_EQ(::truncate(path.c_str(), data.size() - 1), 0);
  {
    Records records;
    Journal journal(path, [&](folly::ByteRange record) {
      records.emplace_back(reinterpret_cast<const char*>(record.data()), record.size());
    });
    EXPECT_EQ(records, Records{"one"});
    journal.append(folly::StringPiece("three"));
  }
  EXPECT_EQ(replay(path), (Records{"one", "three"}));
}

TEST(JournalTest, rewrite) {
  folly::test::TemporaryDirectory dir;
  const auto path = dir.path().string() + "/journal";
  {
    Journal journal(path, [](folly::ByteRange) {});
    journal.append(folly::StringPiece("one"));
    journal.rewrite({"a", "b"});
    journal.append(folly::StringPiece("c"));
  }
  EXPECT_EQ(replay(path), (Records{"a", "b", "c"}));
}

TEST(JournalTest, records) {
  RecordWriter writer;
  writer.u8(7).u64(1ull << 40).string("name").string("");
  RecordReader reader(folly::StringPiece(writer.record()));
  EXPECT_EQ(reader.u8(), 7);
  EXPECT_EQ(reader.u64(), 1ull << 40);
  EXPECT_EQ(reader.string(), "name");
  EXPECT_EQ(reader.string(), "");
  EXPECT_TRUE(reader.done());
  EXPECT_THROW(reader.u8(), Error);
}

} // namespace
} // namespace kiln::meta
