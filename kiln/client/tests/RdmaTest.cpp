// The client's RDMA transport against an in-process cluster whose DRAM
// servers serve RDMA. Skipped without an RDMA device (e.g. Soft-RoCE's rxe0,
// from rlib's scripts/setup-rxe.sh).

#include <cstring>
#include <functional>
#include <stdexcept>
#include <vector>

#include <fmt/format.h>
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
    // Without shared memory, so that RDMA is used on this one host; the
    // metadata calls over TCP (only a tcp:// URL).
    client_ = std::make_unique<Client>(ClientOptions{
        .metadata = cluster_->metadataEndpoint(), .useShm = false});
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

  // Writes 'data' as one chunk with 'client' and reads it back; returns the
  // bytes that went over RDMA.
  static uint64_t writeRead(Client& client, const std::vector<uint8_t>& data) {
    const auto before = client.stats().rdmaBytes;
    auto writer = blockingWait(client.co_create());
    auto buffer = client.allocateBuffer(data.size());
    std::memcpy(buffer.data(), data.data(), data.size());
    buffer.setSize(data.size());
    blockingWait(writer->co_append(std::move(buffer)));
    const auto id = blockingWait(writer->co_commit());
    auto read = blockingWait(client.co_read(
        id, 0, data.size(), client.allocateBuffer(data.size())));
    EXPECT_EQ(std::memcmp(read.data(), data.data(), data.size()), 0);
    return client.stats().rdmaBytes - before;
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

// The whole client with the metadata calls over RDMA too: nothing goes over
// TCP but the storage servers' registrations.
TEST_F(RdmaTest, makesMetadataCallsOverRdma) {
  ASSERT_TRUE(cluster_->metadataAddress().rdma);
  client_ = std::make_unique<Client>(ClientOptions{
      .metadata = cluster_->metadataAddress(), .useShm = false});
  EXPECT_TRUE(client_->stats().rdmaMetadata);
  EXPECT_TRUE(client_->stats().rdmaData);
  const auto data = pattern((4 << 20) + 3, 6);
  const auto id = put(
      data,
      client_->allocateBuffer(1 << 20),
      Placement{.tier = Tier::kDram, .writeThrough = Tier::kLocalFs});
  const auto chunk = blockingWait(client_->co_stat(id));
  ASSERT_EQ(chunk.replicas.size(), 2);
  EXPECT_EQ(chunk.replicas[0].blocks.size(), 5);
  auto read = blockingWait(client_->co_read(
      id, 0, data.size(), client_->allocateBuffer(data.size())));
  EXPECT_EQ(std::memcmp(read.data(), data.data(), data.size()), 0);

  // More refs than fit one page.
  for (int i = 0; i < 40; ++i) {
    blockingWait(client_->co_casRef(
        fmt::format("rdma/{:02}", i), std::nullopt, "value"));
  }
  const auto refs = blockingWait(client_->co_listRefs("rdma/"));
  ASSERT_EQ(refs.size(), 40);
  EXPECT_EQ(refs.back().name, "rdma/39");
  EXPECT_TRUE(blockingWait(client_->co_remove(id)));
}

// Which way the metadata calls and the data go, by the URLs and switches.
TEST_F(RdmaTest, choosesTransports) {
  const auto data = pattern(2 << 20, 7);
  auto roundTrip = [&](Client& client) { return writeRead(client, data); };
  const auto tcp = *cluster_->metadataAddress().tcp;
  const auto rdma = *cluster_->metadataAddress().rdma;

  // tcp:// only: metadata over TCP, data over RDMA.
  Client tcpMetadata(ClientOptions{.metadata = tcp, .useShm = false});
  EXPECT_FALSE(tcpMetadata.stats().rdmaMetadata);
  EXPECT_TRUE(tcpMetadata.stats().rdmaData);
  EXPECT_EQ(roundTrip(tcpMetadata), 2 * data.size());

  // rdma:// only works too.
  MetadataAddress rdmaOnly;
  rdmaOnly.rdma = rdma;
  Client rdmaMetadata(ClientOptions{.metadata = rdmaOnly, .useShm = false});
  EXPECT_TRUE(rdmaMetadata.stats().rdmaMetadata);
  EXPECT_EQ(roundTrip(rdmaMetadata), 2 * data.size());

  // --meta_force_tcp: the rdma:// URL is not used.
  Client metaForceTcp(ClientOptions{
      .metadata = cluster_->metadataAddress(),
      .metaForceTcp = true,
      .useShm = false});
  EXPECT_FALSE(metaForceTcp.stats().rdmaMetadata);
  EXPECT_TRUE(metaForceTcp.stats().rdmaData);
  EXPECT_EQ(roundTrip(metaForceTcp), 2 * data.size());
  try {
    Client none(ClientOptions{.metadata = rdmaOnly, .metaForceTcp = true});
    FAIL() << "No usable metadata URL";
  } catch (const Error& e) {
    EXPECT_EQ(e.code(), ErrorCode::kInvalidArgument);
  }

  // --data_force_tcp: no RDMA (and no shared memory) for data.
  Client dataForceTcp(ClientOptions{
      .metadata = cluster_->metadataAddress(), .dataForceTcp = true});
  EXPECT_TRUE(dataForceTcp.stats().rdmaMetadata);
  EXPECT_FALSE(dataForceTcp.stats().rdmaData);
  EXPECT_EQ(roundTrip(dataForceTcp), 0);

  // With shared memory, regions on this host are mapped instead.
  Client shm(ClientOptions{.metadata = cluster_->metadataAddress()});
  EXPECT_EQ(roundTrip(shm), 0);
}

// An rdma:// URL nobody answers at fails the calls; they do not go over TCP
// instead.
TEST_F(RdmaTest, doesNotFallBackToTcp) {
  auto address = cluster_->metadataAddress();
  address.rdma->port = address.tcp->port;
  Client client(ClientOptions{.metadata = address});
  ASSERT_TRUE(client.stats().rdmaMetadata);
  try {
    blockingWait(client.co_getRef("a"));
    FAIL() << "Reached the metadata server";
  } catch (const Error& e) {
    EXPECT_EQ(e.code(), ErrorCode::kUnavailable);
  }
}

// A cluster whose servers serve TCP only: everything goes over TCP.
TEST_F(RdmaTest, usesTcpWithServersWithoutRdma) {
  local::LocalCluster tcpCluster(local::LocalClusterOptions{
      .dramServers = 1,
      .localFsServers = 0,
      .regionSize = 16 << 20});
  EXPECT_FALSE(tcpCluster.metadataAddress().rdma);
  for (const auto& region : tcpCluster.servers()[0]->regions()) {
    EXPECT_FALSE(region.rdma);
  }
  Client client(ClientOptions{
      .metadata = tcpCluster.metadataAddress(), .useShm = false});
  EXPECT_FALSE(client.stats().rdmaMetadata);
  EXPECT_TRUE(client.stats().rdmaData);
  EXPECT_EQ(writeRead(client, pattern(1 << 20, 8)), 0);
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
