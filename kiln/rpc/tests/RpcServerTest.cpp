// RpcServer: blocked handlers delay other calls but do not fail them, and
// serving fails on a port already taken.

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include <folly/coro/BlockingWait.h>
#include <folly/coro/Task.h>
#include <folly/executors/InlineExecutor.h>
#include <folly/synchronization/SaturatingSemaphore.h>

#include "kiln/common/Error.h"
#include "kiln/rpc/Rpc.h"

namespace kiln::rpc {
namespace {

// Reads of region 0 block their handler thread until release(); other reads
// return 'length' bytes.
class BlockingStorage : public StorageRpc {
 public:
  folly::coro::Task<std::unique_ptr<folly::IOBuf>>
  co_read(RegionId region, uint64_t /*offset*/, uint64_t length) override {
    if (region == 0) {
      released_.wait();
    }
    auto data = folly::IOBuf::create(length);
    data->append(length);
    co_return data;
  }

  folly::coro::Task<void> co_write(
      RegionId /*region*/,
      uint64_t /*offset*/,
      const folly::IOBuf& /*data*/) override {
    co_return;
  }

  void release() {
    released_.post();
  }

 private:
  folly::SaturatingSemaphore<true> released_;
};

class RpcServerTest : public testing::Test {
 protected:
  void SetUp() override {
    server_ = serveStorage(storage_, "", 0);
    client_ = connectStorage(Endpoint{"localhost", server_->port()});
  }

  void TearDown() override {
    // Lets blocked handlers finish, so the server can stop.
    storage_->release();
  }

  // Starts a read, which is sent before start() returns.
  folly::SemiFuture<std::unique_ptr<folly::IOBuf>> start(
      RegionId region, uint64_t length) {
    return folly::coro::co_withExecutor(
               &folly::InlineExecutor::instance(),
               client_->co_read(region, 0, length))
        .start();
  }

  uint64_t read(RegionId region, uint64_t length) {
    return folly::coro::blockingWait(client_->co_read(region, 0, length))
        ->computeChainDataLength();
  }

  const std::shared_ptr<BlockingStorage> storage_{
      std::make_shared<BlockingStorage>()};
  std::unique_ptr<RpcServer> server_;
  std::shared_ptr<StorageRpc> client_;
};

TEST_F(RpcServerTest, blockedHandlersDelayButDoNotFailOtherCalls) {
  // A blocked handler leaves the other handler threads to other calls.
  std::vector<folly::SemiFuture<std::unique_ptr<folly::IOBuf>>> blocked;
  blocked.push_back(start(0, 1));
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_EQ(read(1, 10), 10);

  // With every handler thread blocked, calls wait for one, however long.
  for (int i = 1; i < 4; ++i) {
    blocked.push_back(start(0, 1));
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  auto queued = start(1, 20);
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  EXPECT_FALSE(queued.isReady());
  storage_->release();
  EXPECT_EQ(
      std::move(queued).get(std::chrono::seconds(10))->computeChainDataLength(),
      20);
  for (auto& call : blocked) {
    EXPECT_EQ(
        std::move(call).get(std::chrono::seconds(10))->computeChainDataLength(),
        1);
  }
}

TEST_F(RpcServerTest, servingFailsWhenPortIsTaken) {
  try {
    serveStorage(storage_, "", server_->port());
    FAIL() << "Served on a port already taken";
  } catch (const Error& e) {
    EXPECT_EQ(e.code(), ErrorCode::kUnavailable);
  }
  // The first server still answers.
  EXPECT_EQ(read(1, 10), 10);
}

} // namespace
} // namespace kiln::rpc
