#include <chrono>
#include <cstring>
#include <numeric>
#include <optional>
#include <vector>

#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>
#include <gtest/gtest.h>

#include "kiln/common/Error.h"
#include "kiln/rdma/Device.h"
#include "rlib/endpoint/RdmaActiveEndpointGroup.h"

namespace kiln::rdma {
namespace {

using namespace std::chrono_literals;

using Group = rlib::RdmaActiveEndpointGroup<rlib::RdmaActiveEndpoint>;
using Factory = rlib::RdmaDefaultEndpointFactory<rlib::RdmaActiveEndpoint>;

// The first RDMA device, or nullopt (and the test skipped) if there is none.
std::optional<Device> openDevice() {
  try {
    return Device::open();
  } catch (const Error& e) {
    EXPECT_EQ(e.code(), ErrorCode::kUnavailable);
    return std::nullopt;
  }
}

std::unique_ptr<Group> makeGroup() {
  auto group = std::make_unique<Group>(
      /*polling=*/false, /*maxWR=*/64, /*maxSge=*/1, /*cqSize=*/256);
  // Soft-RoCE drops packets under load.
  group->connParam().retryCount = 7;
  group->connParam().rnrRetryCount = 7;
  group->init(std::make_unique<Factory>(*group));
  return group;
}

TEST(DeviceTest, opensTheDeviceOfAnAddress) {
  auto device = openDevice();
  if (!device) {
    GTEST_SKIP() << "No RDMA device";
  }
  EXPECT_FALSE(device->name().empty());
  EXPECT_NE(device->pd(), nullptr);

  auto same = Device::open(device->host());
  EXPECT_EQ(same.name(), device->name());
  EXPECT_EQ(same.pd(), device->pd());

  try {
    Device::open("127.0.0.1");
    FAIL() << "Loopback has no RDMA device";
  } catch (const Error& e) {
    EXPECT_EQ(e.code(), ErrorCode::kUnavailable);
  }
}

// Memory registered with the device before any endpoint exists works with
// the endpoints connected later: a one-sided read between two such buffers.
TEST(DeviceTest, registersMemoryForEndpoints) {
  auto device = openDevice();
  if (!device) {
    GTEST_SKIP() << "No RDMA device";
  }
  constexpr size_t kSize = 1 << 20;
  std::vector<uint8_t> remote(kSize);
  std::iota(remote.begin(), remote.end(), 0);
  std::vector<uint8_t> local(kSize);
  auto remoteMr = device->registerMemory(remote.data(), remote.size());
  auto localMr = device->registerMemory(local.data(), local.size());
  EXPECT_EQ(localMr.length(), kSize);

  auto serverGroup = makeGroup();
  auto clientGroup = makeGroup();
  auto server = serverGroup->createServerEndpoint();
  server->bind(rlib::SocketAddress::parse(device->host() + ":0"), 16);
  auto client = clientGroup->createEndpoint();
  auto [accepted, unit] = folly::coro::blockingWait(folly::coro::collectAll(
      server->co_accept(), client->co_connect(server->srcAddr(), 2000ms)));
  EXPECT_EQ(client->pd(), device->pd());
  EXPECT_EQ(accepted->pd(), device->pd());

  EXPECT_EQ(
      folly::coro::blockingWait(client->co_read(
          localMr,
          0,
          kSize,
          reinterpret_cast<uint64_t>(remote.data()),
          remoteMr.rkey())),
      0);
  EXPECT_EQ(local, remote);

  folly::coro::blockingWait(clientGroup->co_close());
  folly::coro::blockingWait(serverGroup->co_close());
}

} // namespace
} // namespace kiln::rdma
