// fbthrift with coroutines on both ends: an in-process server whose handler
// is a coroutine, called from a folly::coro::Task client.
//
// The client awaits semifuture_ping, not the generated co_ping: co_ping is a
// coroutine template, and GCC 11 miscompiles co_await in coroutine templates
// with this folly (see README.md).

#include <gtest/gtest.h>

#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>
#include <folly/coro/Task.h>
#include <thrift/lib/cpp2/util/ScopedServerInterfaceThread.h>

#include "kiln/rpc/gen-cpp2/MetadataService.h"

namespace kiln::rpc {
namespace {

class PingHandler
    : public apache::thrift::ServiceHandler<thrift::MetadataService> {
 public:
  folly::coro::Task<std::unique_ptr<std::string>> co_ping(
      std::unique_ptr<std::string> message) override {
    co_return std::move(message);
  }
};

TEST(PingTest, coroutineCall) {
  apache::thrift::ScopedServerInterfaceThread server(
      std::make_shared<PingHandler>());
  auto client =
      server.newClient<apache::thrift::Client<thrift::MetadataService>>();

  auto reply = folly::coro::blockingWait(
      [&]() -> folly::coro::Task<std::string> {
        co_return co_await client->semifuture_ping("hello kiln");
      }());
  EXPECT_EQ(reply, "hello kiln");
}

TEST(PingTest, concurrentCalls) {
  apache::thrift::ScopedServerInterfaceThread server(
      std::make_shared<PingHandler>());
  auto client =
      server.newClient<apache::thrift::Client<thrift::MetadataService>>();

  auto ping = [&](int i) -> folly::coro::Task<std::string> {
    co_return co_await client->semifuture_ping(std::to_string(i));
  };
  std::vector<folly::coro::Task<std::string>> calls;
  for (int i = 0; i < 16; ++i) {
    calls.push_back(ping(i));
  }
  auto replies =
      folly::coro::blockingWait(folly::coro::collectAllRange(std::move(calls)));
  ASSERT_EQ(replies.size(), 16);
  for (int i = 0; i < 16; ++i) {
    EXPECT_EQ(replies[i], std::to_string(i));
  }
}

} // namespace
} // namespace kiln::rpc
