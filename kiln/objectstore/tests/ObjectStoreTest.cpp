#include <gtest/gtest.h>

#include <folly/coro/BlockingWait.h>
#include <folly/testing/TestUtil.h>

#include "kiln/common/Error.h"
#include "kiln/objectstore/ObjectStore.h"

namespace kiln::objectstore {
namespace {

using folly::coro::blockingWait;

class ObjectStoreTest : public testing::Test {
 protected:
  void put(const std::string& key, folly::StringPiece data) {
    blockingWait(store_->co_put(key, folly::ByteRange(data)));
  }

  std::string get(const std::string& key, uint64_t offset = 0, size_t length = 64) {
    std::string data(length, '\0');
    auto n = blockingWait(store_->co_get(
        key,
        offset,
        folly::MutableByteRange(
            reinterpret_cast<uint8_t*>(data.data()), data.size())));
    data.resize(n);
    return data;
  }

  ErrorCode errorOf(const std::string& key) {
    try {
      get(key);
    } catch (const Error& e) {
      return e.code();
    }
    ADD_FAILURE() << "No error";
    return ErrorCode::kInternal;
  }

  folly::test::TemporaryDirectory dir_;
  std::unique_ptr<ObjectStore> store_ =
      makeLocalObjectStore(dir_.path().string() + "/objects");
};

TEST_F(ObjectStoreTest, putGetHeadRemove) {
  put("chunks/abc", "hello world");
  EXPECT_EQ(get("chunks/abc"), "hello world");
  EXPECT_EQ(get("chunks/abc", 6, 3), "wor");
  EXPECT_EQ(get("chunks/abc", 20), "");
  EXPECT_EQ(blockingWait(store_->co_head("chunks/abc")), 11);

  put("chunks/abc", "bye");
  EXPECT_EQ(get("chunks/abc"), "bye");

  EXPECT_TRUE(blockingWait(store_->co_remove("chunks/abc")));
  EXPECT_FALSE(blockingWait(store_->co_remove("chunks/abc")));
  EXPECT_EQ(blockingWait(store_->co_head("chunks/abc")), std::nullopt);
  EXPECT_EQ(errorOf("chunks/abc"), ErrorCode::kNotFound);
  EXPECT_EQ(errorOf("chunks"), ErrorCode::kNotFound);
}

TEST_F(ObjectStoreTest, list) {
  put("b", "1");
  put("a/y", "2");
  put("a/x", "3");
  put("ab", "4");
  using Keys = std::vector<std::string>;
  EXPECT_EQ(blockingWait(store_->co_list("")), (Keys{"a/x", "a/y", "ab", "b"}));
  EXPECT_EQ(blockingWait(store_->co_list("a/")), (Keys{"a/x", "a/y"}));
  EXPECT_EQ(blockingWait(store_->co_list("a")), (Keys{"a/x", "a/y", "ab"}));
  EXPECT_EQ(blockingWait(store_->co_list("c")), Keys{});
}

TEST_F(ObjectStoreTest, badKeys) {
  for (const std::string key :
       {"", "/a", "a/", "a//b", "../a", "a/./b", ".tmp", ".tmp/x"}) {
    EXPECT_EQ(errorOf(key), ErrorCode::kInvalidArgument) << key;
  }
}

} // namespace
} // namespace kiln::objectstore
