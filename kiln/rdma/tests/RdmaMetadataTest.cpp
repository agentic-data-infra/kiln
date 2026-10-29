// The metadata calls over RDMA against a metadata server's state in this
// process. Skipped without an RDMA device.

#include <optional>

#include <folly/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "kiln/common/Error.h"
#include "kiln/meta/Metadata.h"
#include "kiln/rdma/Device.h"
#include "kiln/rdma/RdmaMetadata.h"

namespace kiln::rdma {
namespace {

using folly::coro::blockingWait;

class RdmaMetadataTest : public testing::Test {
 protected:
  void SetUp() override {
    try {
      Device::open();
    } catch (const Error&) {
      GTEST_SKIP() << "No RDMA device";
    }
    metadata_ = std::make_shared<meta::Metadata>(meta::MetadataOptions{
        .gracePeriod = std::chrono::milliseconds(0)});
    server_ = RdmaMetadataServer::start(metadata_, "", 0);
    client_ = connectRdmaMetadata(server_->endpoint());
  }

  void TearDown() override {
    client_.reset();
    server_.reset();
  }

  template <typename T>
  static ErrorCode errorOf(folly::coro::Task<T> task) {
    try {
      blockingWait(std::move(task));
    } catch (const Error& e) {
      return e.code();
    }
    ADD_FAILURE() << "No error";
    return ErrorCode::kInternal;
  }

  std::shared_ptr<meta::Metadata> metadata_;
  std::unique_ptr<RdmaMetadataServer> server_;
  std::shared_ptr<rpc::MetadataRpc> client_;
};

// Every call, against the same state the TCP server would serve.
TEST_F(RdmaMetadataTest, makesAllCalls) {
  const RegionInfo offered{
      .server = "storage:1",
      .tier = Tier::kDram,
      .length = 8 * kBlockSize,
      .tcp = TcpAccess{.endpoint = {"storage", 1}},
      .rdma = RdmaAccess{
          .endpoint = {"10.0.0.1", 2}, .rkey = 3, .baseAddress = 4096}};
  const auto regions = blockingWait(client_->co_registerServer({offered}, 0));
  ASSERT_EQ(regions.size(), 1);
  EXPECT_NE(regions[0].id, 0);
  EXPECT_EQ(regions[0].rdma, offered.rdma);
  EXPECT_EQ(blockingWait(client_->co_getRegions({regions[0].id})), regions);

  auto blocks = blockingWait(client_->co_allocate(Tier::kDram, 3));
  ASSERT_EQ(blocks.size(), 3);
  EXPECT_EQ(metadata_->freeBlocks(Tier::kDram), 5);
  blocks.back().length = 100;
  const auto spare = blockingWait(client_->co_allocate(Tier::kDram, 1));
  blockingWait(client_->co_free(spare));
  EXPECT_EQ(metadata_->freeBlocks(Tier::kDram), 5);

  ChunkInfo chunk{
      .id = ChunkId{.high = 1, .low = 2},
      .size = 2 * kBlockSize + 100,
      .replicas = {Replica{.tier = Tier::kDram, .blocks = blocks}}};
  EXPECT_EQ(blockingWait(client_->co_commit(chunk)), chunk);
  EXPECT_EQ(blockingWait(client_->co_lookup(chunk.id)), chunk);
  EXPECT_TRUE(blockingWait(client_->co_remove(chunk.id)));
  EXPECT_FALSE(blockingWait(client_->co_remove(chunk.id)));
  EXPECT_EQ(errorOf(client_->co_lookup(chunk.id)), ErrorCode::kNotFound);

  const auto job = blockingWait(client_->co_createBucket());
  EXPECT_NE(job, kDefaultBucket);
  chunk.bucket = job;
  chunk.replicas[0].blocks = blockingWait(client_->co_allocate(Tier::kDram, 3));
  chunk.replicas[0].blocks.back().length = 100;
  EXPECT_EQ(blockingWait(client_->co_commit(chunk)), chunk);
  EXPECT_TRUE(blockingWait(client_->co_deleteBucket(job)));
  EXPECT_FALSE(blockingWait(client_->co_deleteBucket(job)));
  EXPECT_EQ(errorOf(client_->co_lookup(chunk.id)), ErrorCode::kNotFound);

  EXPECT_EQ(blockingWait(client_->co_getRef("a")), std::nullopt);
  const auto a = blockingWait(client_->co_casRef("a", std::nullopt, "one"));
  ASSERT_TRUE(a);
  EXPECT_EQ(blockingWait(client_->co_getRef("a")), a);
  EXPECT_EQ(
      blockingWait(client_->co_casRef("a", a->version + 1, "two")),
      std::nullopt);
  blockingWait(client_->co_casRef("b", std::nullopt, "b"));
  const auto listed = blockingWait(client_->co_listRefs("", "a", 1));
  ASSERT_EQ(listed.size(), 1);
  EXPECT_EQ(listed[0].name, "b");
  EXPECT_FALSE(blockingWait(client_->co_removeRef("a", a->version + 1)));
  EXPECT_TRUE(blockingWait(client_->co_removeRef("a", std::nullopt)));
  EXPECT_EQ(
      errorOf(client_->co_casRef("", std::nullopt, "x")),
      ErrorCode::kInvalidArgument);
}

// Requests and responses that do not fit a message fail with kTooLarge; the
// connection stays usable.
TEST_F(RdmaMetadataTest, failsCallsThatDoNotFit) {
  std::vector<RegionId> ids(10'000, 1);
  EXPECT_EQ(errorOf(client_->co_getRegions(ids)), ErrorCode::kTooLarge);

  // 100 refs of 1 KiB: too many for one response, fine in pages.
  for (int i = 0; i < 100; ++i) {
    blockingWait(client_->co_casRef(
        fmt::format("ref{:03}", i),
        std::nullopt,
        std::string(kMaxRefValueSize, 'v')));
  }
  EXPECT_EQ(errorOf(client_->co_listRefs("", "", 0)), ErrorCode::kTooLarge);
  EXPECT_EQ(blockingWait(client_->co_listRefs("", "ref049", 32)).size(), 32);
}

// A call fails with kUnavailable while the server is down, and works again
// once it is back, at the same port.
TEST_F(RdmaMetadataTest, reconnectsToARestartedServer) {
  blockingWait(client_->co_casRef("a", std::nullopt, "one"));
  const auto endpoint = server_->endpoint();
  server_.reset();
  EXPECT_EQ(errorOf(client_->co_getRef("a")), ErrorCode::kUnavailable);

  server_ = RdmaMetadataServer::start(metadata_, endpoint.host, endpoint.port);
  const auto ref = blockingWait(client_->co_getRef("a"));
  ASSERT_TRUE(ref);
  EXPECT_EQ(ref->value, "one");
}

} // namespace
} // namespace kiln::rdma
