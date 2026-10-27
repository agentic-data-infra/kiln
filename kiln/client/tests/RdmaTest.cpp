// The client's RDMA transport against an in-process cluster whose DRAM
// servers serve RDMA. Skipped without an RDMA device (e.g. Soft-RoCE's rxe0,
// from rlib's scripts/setup-rxe.sh).

#include <cstring>
#include <functional>
#include <stdexcept>
#include <vector>

#include <folly/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "kiln/client/Client.h"
#include "kiln/client/RdmaTransport.h"
#include "kiln/common/Error.h"
#include "kiln/local/LocalCluster.h"
#include "kiln/rdma/Device.h"

namespace kiln::client {
namespace {

using folly::coro::blockingWait;

bool haveDevice() {
  try {
    rdma::Device::open();
    return true;
  } catch (const Error&) {
    return false;
  }
}

// 'size' bytes of a pattern that differs with 'seed'.
std::vector<uint8_t> pattern(size_t size, uint32_t seed = 1) {
  std::vector<uint8_t> data(size);
  uint32_t x = seed * 2654435761u + 1;
  for (auto& b : data) {
    x = x * 1664525u + 1013904223u;
    b = static_cast<uint8_t>(x >> 24);
  }
  return data;
}

class RdmaTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    if (!haveDevice()) {
      return;
    }
    cluster_ = new local::LocalCluster(local::LocalClusterOptions{
        .dramServers = 2,
        .localFsServers = 1,
        .regionsPerServer = 2,
        .regionSize = 16 << 20,
        .metadata = meta::MetadataOptions{
            .gracePeriod = std::chrono::milliseconds(0),
            .evictBelow = 0,
            .evictUntil = 0},
        .rdma = true});
  }

  static void TearDownTestSuite() {
    delete cluster_;
    cluster_ = nullptr;
  }

  void SetUp() override {
    if (cluster_ == nullptr) {
      GTEST_SKIP() << "No RDMA device";
    }
    // Without shared memory, so that RDMA is used on this one host.
    client_ = std::make_unique<Client>(ClientOptions{
        .metadata = cluster_->metadataEndpoint(),
        .useShm = false,
        .useRdma = true});
  }

  // Writes 'data' as one chunk from 'buffer', appended in pieces.
  ChunkId put(
      const std::vector<uint8_t>& data,
      Buffer buffer,
      Placement placement = {}) {
    auto writer = blockingWait(client_->co_create(placement));
    const auto piece = buffer.capacity();
    for (size_t done = 0; done < data.size(); done += piece) {
      const auto n = std::min(piece, data.size() - done);
      std::memcpy(buffer.data(), data.data() + done, n);
      buffer.setSize(n);
      buffer = blockingWait(writer->co_append(std::move(buffer)));
    }
    return blockingWait(writer->co_commit());
  }

  // The region as its storage server registered it.
  static RegionInfo region(RegionId id) {
    for (const auto& server : cluster_->servers()) {
      for (const auto& region : server->regions()) {
        if (region.id == id) {
          return region;
        }
      }
    }
    throw std::out_of_range("No such region");
  }

  static local::LocalCluster* cluster_;
  std::unique_ptr<Client> client_;
};

local::LocalCluster* RdmaTest::cluster_ = nullptr;

TEST_F(RdmaTest, dramRegionsHaveRdmaAccess) {
  for (const auto& server : cluster_->servers()) {
    for (const auto& region : server->regions()) {
      if (region.tier == Tier::kDram) {
        ASSERT_TRUE(region.rdma.has_value());
        EXPECT_NE(region.rdma->rkey, 0);
        EXPECT_NE(region.rdma->baseAddress, 0);
        EXPECT_NE(region.rdma->endpoint.port, 0);
      } else {
        EXPECT_FALSE(region.rdma.has_value());
      }
    }
  }
}

// Registered buffers: the data moves straight to and from them.
TEST_F(RdmaTest, writesAndReadsRegisteredBuffersWithoutCopies) {
  const auto data = pattern((5 << 20) + 12345);
  const auto id = put(data, client_->allocateBuffer(3 << 20));

  auto read = blockingWait(client_->co_read(
      id, 0, data.size(), client_->allocateBuffer(data.size())));
  ASSERT_EQ(read.size(), data.size());
  EXPECT_EQ(std::memcmp(read.data(), data.data(), data.size()), 0);

  // A range across blocks, into the caller's own registered memory.
  std::vector<uint8_t> memory(3 << 20);
  const uint64_t offset = (1 << 20) - 100;
  auto range = blockingWait(client_->co_read(
      id,
      offset,
      memory.size(),
      client_->registerMemory(memory.data(), memory.size())));
  ASSERT_EQ(range.size(), memory.size());
  EXPECT_EQ(std::memcmp(memory.data(), data.data() + offset, memory.size()), 0);

  const auto stats = client_->stats();
  EXPECT_EQ(stats.rdmaBytes, 2 * data.size() + memory.size());
  EXPECT_EQ(stats.rdmaCopiedBytes, 0);
}

// Memory that is not registered works too, through a copy.
TEST_F(RdmaTest, copiesUnregisteredMemory) {
  const auto data = pattern((2 << 20) + 7, 2);
  const auto id = put(data, Buffer::allocate(1 << 20));
  auto read = blockingWait(
      client_->co_read(id, 0, data.size(), Buffer::allocate(data.size())));
  EXPECT_EQ(std::memcmp(read.data(), data.data(), data.size()), 0);

  const auto stats = client_->stats();
  EXPECT_EQ(stats.rdmaBytes, 0);
  EXPECT_EQ(stats.rdmaCopiedBytes, 2 * data.size());
}

// A buffer's registration goes with it; its memory is reusable.
TEST_F(RdmaTest, reusesAndReleasesBuffers) {
  const auto data = pattern(3 << 20, 3);
  const auto id = put(data, client_->allocateBuffer(1 << 20));
  auto buffer = client_->allocateBuffer(1 << 20);
  for (int i = 0; i < 3; ++i) {
    buffer = blockingWait(
        client_->co_read(id, i << 20, 1 << 20, std::move(buffer)));
    EXPECT_EQ(std::memcmp(buffer.data(), data.data() + (i << 20), 1 << 20), 0);
    // Replaced by a new buffer: the old one's registration goes first.
    buffer = client_->allocateBuffer(1 << 20);
  }
  EXPECT_EQ(client_->stats().rdmaCopiedBytes, 0);
}

// Local-FS regions have no RDMA access and are reached over TCP.
TEST_F(RdmaTest, readsLocalFsOverTcp) {
  const auto data = pattern(1 << 20, 4);
  const auto id = put(
      data, client_->allocateBuffer(1 << 20), Placement{.tier = Tier::kLocalFs});
  auto read = blockingWait(client_->co_read(
      id, 0, data.size(), client_->allocateBuffer(data.size())));
  EXPECT_EQ(std::memcmp(read.data(), data.data(), data.size()), 0);
  EXPECT_EQ(client_->stats().rdmaBytes, 0);
}

// Against the transport itself, which can be given broken RdmaAccess: a
// failed operation fails with kUnavailable, and the next one connects again.
class RdmaTransportTest : public RdmaTest {
 protected:
  void SetUp() override {
    RdmaTest::SetUp();
    if (IsSkipped()) {
      return;
    }
    const auto id = put(data_, client_->allocateBuffer(1 << 20));
    block_ = blockingWait(client_->co_stat(id)).replicas[0].blocks[0];
    region_ = region(block_.region);
    memory_ = std::make_shared<RdmaMemory>(rdma::Device::open());
    stats_ = std::make_shared<RdmaStats>();
    transport_ = makeRdmaTransport(memory_, stats_);
    buffer_ = Buffer::allocate(block_.length);
    buffer_.setRegistration(
        memory_->registerMemory(buffer_.data(), block_.length));
  }

  void read(const RegionInfo& region) {
    std::memset(buffer_.data(), 0, block_.length);
    blockingWait(transport_->co_read(
        region,
        block_.offset,
        folly::MutableByteRange(buffer_.data(), block_.length)));
  }

  void write(const RegionInfo& region) {
    std::memcpy(buffer_.data(), data_.data(), block_.length);
    blockingWait(transport_->co_write(
        region, block_.offset, folly::ByteRange(buffer_.data(), block_.length)));
  }

  bool readsTheBlock() {
    return std::memcmp(buffer_.data(), data_.data(), block_.length) == 0;
  }

  void expectUnavailable(std::function<void()> call) {
    try {
      call();
      FAIL() << "Expected kUnavailable";
    } catch (const Error& e) {
      EXPECT_EQ(e.code(), ErrorCode::kUnavailable) << e.what();
    }
  }

  const std::vector<uint8_t> data_ = pattern(1 << 20, 5);
  BlockRef block_;
  RegionInfo region_;
  std::shared_ptr<RdmaMemory> memory_;
  std::shared_ptr<RdmaStats> stats_;
  std::unique_ptr<Transport> transport_;
  Buffer buffer_;
};

// A wrong key fails the operation, and with it the connection (its QP goes
// into the error state); the next operations connect again.
TEST_F(RdmaTransportTest, reconnectsAfterAFailedOperation) {
  read(region_);
  EXPECT_TRUE(readsTheBlock());

  auto wrongKey = region_;
  wrongKey.rdma->rkey += 1;
  expectUnavailable([&] { read(wrongKey); });
  read(region_);
  EXPECT_TRUE(readsTheBlock());

  expectUnavailable([&] { write(wrongKey); });
  write(region_);
  read(region_);
  EXPECT_TRUE(readsTheBlock());
  EXPECT_EQ(stats_->bounced, 0);
}

// A server that cannot be reached fails the operation; reaching it again
// works.
TEST_F(RdmaTransportTest, failsToConnectToNoServer) {
  auto noServer = region_;
  // Nothing listens on the TCP port in RDMA's port space.
  noServer.rdma->endpoint.port = region_.tcp->endpoint.port;
  expectUnavailable([&] { read(noServer); });
  read(region_);
  EXPECT_TRUE(readsTheBlock());
}

// Clients outlive their buffers and buffers their clients.
TEST_F(RdmaTest, buffersOutliveTheClient) {
  auto buffer = client_->allocateBuffer(1 << 20);
  client_.reset();
  buffer = Buffer();
}

} // namespace
} // namespace kiln::client
