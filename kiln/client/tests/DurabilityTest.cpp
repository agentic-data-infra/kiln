// Written-through chunks: their copies in the write-through tier serve reads
// when the first copy is evicted, and outlive restarts of the metadata
// server (with its journal) and of local-FS storage servers (with their own
// directories).

#include <gtest/gtest.h>

#include <thread>

#include <folly/coro/BlockingWait.h>
#include <folly/testing/TestUtil.h>

#include "kiln/client/Client.h"
#include "kiln/common/Error.h"
#include "kiln/meta/Metadata.h"
#include "kiln/objectstore/ObjectStore.h"
#include "kiln/rpc/Rpc.h"
#include "kiln/server/StorageServer.h"

namespace kiln::client {
namespace {

using folly::coro::blockingWait;
using namespace std::chrono_literals;

std::string bytes(size_t size, char seed) {
  std::string data(size, '\0');
  for (size_t i = 0; i < size; ++i) {
    data[i] = static_cast<char>(seed + i * 7 + (i >> 12));
  }
  return data;
}

class DurabilityTest : public testing::Test {
 protected:
  ~DurabilityTest() override {
    client_.reset();
    dram_.reset();
    localFs_.reset();
    stopMetadata();
  }

  void startMetadata() {
    metadata_ = std::make_shared<meta::Metadata>(meta::MetadataOptions{
        .gracePeriod = 0ms,
        .evictBelow = 0,
        .evictUntil = 0,
        .journal = dir_.path().string() + "/journal"});
    rpcServer_ = rpc::serveMetadata(metadata_, "127.0.0.1", port_);
    port_ = rpcServer_->port();
  }

  void stopMetadata() {
    rpcServer_.reset();
    metadata_.reset();
  }

  // A DRAM server of 'dramMb' and a local-FS server of 16 MiB in its own
  // directory, and a client.
  void startServers(uint64_t dramMb = 16) {
    auto start = [&](Tier tier, uint64_t size, std::string dir) {
      return blockingWait(
          server::StorageServer::co_start(server::StorageServerOptions{
              .metadata = endpoint(),
              .bindHost = "127.0.0.1",
              .host = "127.0.0.1",
              .tier = tier,
              .regionSize = size,
              .dir = std::move(dir),
              .connectTimeout = 10s,
              .registerInterval = 100ms}));
    };
    dram_ = start(Tier::kDram, dramMb << 20, "");
    localFs_ = start(Tier::kLocalFs, 16 << 20, dir_.path().string() + "/fs");
    client_ = std::make_unique<Client>(ClientOptions{
        .metadata = endpoint(),
        .chunkCacheTtl = 0ms,
        .objectStore = objectStore_});
  }

  void stopServers() {
    client_.reset();
    dram_.reset();
    localFs_.reset();
  }

  Endpoint endpoint() const {
    return Endpoint{"127.0.0.1", port_};
  }

  ChunkId put(const std::string& data, Placement placement) {
    auto writer = blockingWait(client_->co_create(kDefaultBucket, placement));
    auto buffer = client_->allocateBuffer(data.size());
    std::memcpy(buffer.data(), data.data(), data.size());
    buffer.setSize(data.size());
    blockingWait(writer->co_append(std::move(buffer)));
    return blockingWait(writer->co_commit());
  }

  std::string get(ChunkId id) {
    const auto size = blockingWait(client_->co_stat(id)).size;
    auto buffer = blockingWait(
        client_->co_read(id, 0, size, client_->allocateBuffer(size)));
    return std::string(
        reinterpret_cast<const char*>(buffer.data()), buffer.size());
  }

  std::vector<Tier> tiersOf(ChunkId id) {
    std::vector<Tier> tiers;
    for (const auto& replica : blockingWait(client_->co_stat(id)).replicas) {
      tiers.push_back(replica.tier);
    }
    return tiers;
  }

  // What co_stat fails with, if it does.
  std::optional<ErrorCode> statError(ChunkId id) {
    try {
      blockingWait(client_->co_stat(id));
    } catch (const Error& e) {
      return e.code();
    }
    return std::nullopt;
  }

  folly::test::TemporaryDirectory dir_;
  std::shared_ptr<objectstore::ObjectStore> objectStore_ =
      objectstore::makeLocalObjectStore(dir_.path().string() + "/objects");
  uint16_t port_{0};
  std::shared_ptr<meta::Metadata> metadata_;
  std::unique_ptr<rpc::RpcServer> rpcServer_;
  std::unique_ptr<server::StorageServer> dram_;
  std::unique_ptr<server::StorageServer> localFs_;
  std::unique_ptr<Client> client_;
};

const Placement kToLocalFs{.tier = Tier::kDram, .writeThrough = Tier::kLocalFs};
const Placement kToObjectStore{
    .tier = Tier::kDram, .writeThrough = Tier::kObjectStore};

TEST_F(DurabilityTest, readsFallBackToTheWriteThroughCopy) {
  startMetadata();
  startServers(/*dramMb=*/4);
  const auto a = bytes(3 << 20, 'a');
  const auto b = bytes(3 << 20, 'b');
  const auto c = bytes(3 << 20, 'c');
  const auto idA = put(a, kToLocalFs);
  EXPECT_EQ(tiersOf(idA), (std::vector{Tier::kDram, Tier::kLocalFs}));
  // C evicts the DRAM copy of A, and B that of C.
  const auto idC = put(c, kToObjectStore);
  EXPECT_EQ(tiersOf(idC), (std::vector{Tier::kDram, Tier::kObjectStore}));
  EXPECT_EQ(
      blockingWait(objectStore_->co_head(chunkObjectKey(idC))), c.size());

  const auto idB = put(b, Placement{});
  EXPECT_EQ(tiersOf(idA), std::vector{Tier::kLocalFs});
  EXPECT_EQ(tiersOf(idC), std::vector{Tier::kObjectStore});
  EXPECT_EQ(get(idA), a);
  EXPECT_EQ(get(idB), b);
  EXPECT_EQ(get(idC), c);

  // Writing A again brings back its copy in DRAM.
  EXPECT_EQ(put(a, kToLocalFs), idA);
  EXPECT_EQ(tiersOf(idA), (std::vector{Tier::kDram, Tier::kLocalFs}));
  EXPECT_EQ(get(idA), a);
}

TEST_F(DurabilityTest, survivesRestarts) {
  startMetadata();
  startServers();
  const auto a = bytes(3 << 20, 'a');
  const auto b = bytes(2 << 20, 'b');
  const auto idA = put(a, kToLocalFs);
  const auto idB = put(b, kToObjectStore);
  const auto ephemeralDram = put(bytes(1 << 20, 'c'), Placement{});
  const auto ephemeralLocalFs =
      put(bytes(1 << 20, 'd'), Placement{.tier = Tier::kLocalFs});

  stopServers();
  stopMetadata();
  startMetadata();
  startServers();

  EXPECT_EQ(tiersOf(idA), std::vector{Tier::kLocalFs});
  EXPECT_EQ(get(idA), a);
  EXPECT_EQ(tiersOf(idB), std::vector{Tier::kObjectStore});
  EXPECT_EQ(get(idB), b);
  EXPECT_EQ(statError(ephemeralDram), ErrorCode::kNotFound);
  EXPECT_EQ(statError(ephemeralLocalFs), ErrorCode::kNotFound);

  // New chunks fill the local FS around A's blocks.
  const auto e = bytes(12 << 20, 'e');
  const auto idE = put(e, Placement{.tier = Tier::kLocalFs});
  EXPECT_EQ(get(idE), e);
  EXPECT_EQ(get(idA), a);
}

TEST_F(DurabilityTest, storageServersRegisterWithARestartedMetadataServer) {
  startMetadata();
  startServers();
  const auto a = bytes(3 << 20, 'a');
  const auto idA = put(a, kToLocalFs);

  stopMetadata();
  startMetadata();
  // Until the local-FS server registers again, A cannot be read.
  EXPECT_EQ(statError(idA), ErrorCode::kUnavailable);
  for (int i = 0; i < 100 && statError(idA); ++i) {
    std::this_thread::sleep_for(50ms);
  }
  EXPECT_EQ(tiersOf(idA), std::vector{Tier::kLocalFs});
  EXPECT_EQ(get(idA), a);
  // The DRAM server registered again too.
  const auto b = bytes(1 << 20, 'b');
  EXPECT_EQ(get(put(b, Placement{})), b);
}

} // namespace
} // namespace kiln::client
