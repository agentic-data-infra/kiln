#include <fmt/format.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/io/async/AsyncSocket.h>
#include <thrift/lib/cpp2/async/PooledRequestChannel.h>
#include <thrift/lib/cpp2/async/RocketClientChannel.h>

#include "kiln/rpc/Convert.h"
#include "kiln/rpc/Rpc.h"
#include "kiln/rpc/gen-cpp2/MetadataServiceAsyncClient.h"
#include "kiln/rpc/gen-cpp2/StorageServiceAsyncClient.h"

namespace kiln::rpc {

namespace {

constexpr int kConnectTimeoutMs = 10'000;

// The event base threads of every connection in the process.
std::weak_ptr<folly::IOExecutor> ioExecutor() {
  static auto* pool = new std::shared_ptr<folly::IOThreadPoolExecutor>(
      std::make_shared<folly::IOThreadPoolExecutor>(
          4, std::make_shared<folly::NamedThreadFactory>("KilnRpcIO")));
  return *pool;
}

// A generated client of 'Service' at 'endpoint', usable from any thread: its
// channel runs on the IO pool and connects (again) when needed.
template <typename Service>
std::unique_ptr<apache::thrift::Client<Service>> makeClient(
    const Endpoint& endpoint) {
  folly::SocketAddress address;
  try {
    address.setFromHostPort(endpoint.host, endpoint.port);
  } catch (const std::exception& e) {
    throw Error(
        ErrorCode::kUnavailable,
        fmt::format("Cannot resolve {}: {}", endpoint.toString(), e.what()));
  }
  auto channel = apache::thrift::PooledRequestChannel::newSyncChannel(
      ioExecutor(), [address](folly::EventBase& eventBase) {
        auto socket =
            folly::AsyncSocket::newSocket(&eventBase, address, kConnectTimeoutMs);
        auto channel =
            apache::thrift::RocketClientChannel::newChannel(std::move(socket));
        // Calls take as long as they take; callers bound their own waits.
        channel->setTimeout(0);
        return channel;
      });
  return std::make_unique<apache::thrift::Client<Service>>(std::move(channel));
}

// A generated client of 'Service' at 'endpoint' that connects again after
// its connection broke, e.g. when the server restarted.
template <typename Service>
class Connection {
 public:
  using Client = apache::thrift::Client<Service>;

  explicit Connection(const Endpoint& endpoint)
      : endpoint_{endpoint}, client_{makeClient<Service>(endpoint)} {}

  const Endpoint& endpoint() const {
    return endpoint_;
  }

  std::shared_ptr<Client> client() {
    std::lock_guard lock(mutex_);
    return client_;
  }

  // Awaits a call of 'client', turning its failures into kiln::Error.
  template <typename T>
  folly::coro::Task<T> translated(
      std::shared_ptr<Client> client,
      folly::coro::Task<T> call) {
    try {
      co_return co_await std::move(call);
    } catch (const thrift::KilnException& e) {
      throw fromThrift(e);
    } catch (const apache::thrift::transport::TTransportException& e) {
      reconnect(client);
      throw Error(
          ErrorCode::kUnavailable,
          fmt::format("Cannot reach {}: {}", endpoint_.toString(), e.what()));
    } catch (const Error&) {
      throw;
    } catch (const std::exception& e) {
      throw Error(
          ErrorCode::kInternal,
          fmt::format("Call to {} failed: {}", endpoint_.toString(), e.what()));
    }
  }

 private:
  // Replaces a client whose connection broke, unless another call did.
  void reconnect(const std::shared_ptr<Client>& broken) {
    std::lock_guard lock(mutex_);
    if (client_ == broken) {
      client_ = makeClient<Service>(endpoint_);
    }
  }

  const Endpoint endpoint_;
  std::mutex mutex_;
  std::shared_ptr<Client> client_;
};

class RemoteMetadata : public MetadataRpc {
 public:
  explicit RemoteMetadata(const Endpoint& endpoint)
      : connection_{endpoint} {}

  folly::coro::Task<std::vector<RegionInfo>> co_registerServer(
      std::vector<RegionInfo> regions,
      uint64_t budget) override {
    const auto request = toThrift(regions);
    auto client = connection_.client();
    co_return fromThrift(co_await connection_.translated(
        client,
        client->co_registerServer(request, static_cast<int64_t>(budget))));
  }

  folly::coro::Task<std::vector<RegionInfo>> co_getRegions(
      std::vector<RegionId> ids) override {
    const std::vector<int64_t> request(ids.begin(), ids.end());
    auto client = connection_.client();
    co_return fromThrift(
        co_await connection_.translated(
            client, client->co_getRegions(request)));
  }

  folly::coro::Task<std::vector<BlockRef>> co_allocate(
      Tier tier,
      uint32_t count,
      std::optional<Tier> overflow) override {
    std::vector<thrift::Tier> overflowTiers;
    if (overflow) {
      overflowTiers.push_back(static_cast<thrift::Tier>(*overflow));
    }
    auto client = connection_.client();
    co_return fromThrift(co_await connection_.translated(
        client,
        client->co_allocate(
            static_cast<thrift::Tier>(tier),
            static_cast<int32_t>(count),
            overflowTiers)));
  }

  folly::coro::Task<void> co_free(std::vector<BlockRef> blocks) override {
    const auto request = toThrift(blocks);
    auto client = connection_.client();
    co_await connection_.translated(client, client->co_free(request));
  }

  folly::coro::Task<ChunkInfo> co_commit(ChunkInfo chunk) override {
    const auto request = toThrift(chunk);
    auto client = connection_.client();
    co_return fromThrift(
        co_await connection_.translated(client, client->co_commit(request)));
  }

  folly::coro::Task<ChunkInfo> co_lookup(ChunkId id) override {
    const auto request = toThrift(id);
    auto client = connection_.client();
    co_return fromThrift(
        co_await connection_.translated(client, client->co_lookup(request)));
  }

  folly::coro::Task<bool> co_remove(ChunkId id) override {
    const auto request = toThrift(id);
    auto client = connection_.client();
    co_return co_await connection_.translated(
        client, client->co_remove(request));
  }

  folly::coro::Task<BucketId> co_createBucket() override {
    auto client = connection_.client();
    co_return static_cast<BucketId>(co_await connection_.translated(
        client, client->co_createBucket()));
  }

  folly::coro::Task<bool> co_deleteBucket(
      BucketId bucket,
      std::chrono::milliseconds grace) override {
    auto client = connection_.client();
    co_return co_await connection_.translated(
        client,
        client->co_deleteBucket(
            static_cast<int64_t>(bucket), static_cast<int64_t>(grace.count())));
  }

  folly::coro::Task<std::optional<Ref>> co_getRef(std::string name) override {
    auto client = connection_.client();
    co_return fromThrift(
        co_await connection_.translated(client, client->co_getRef(name)));
  }

  folly::coro::Task<std::optional<Ref>> co_casRef(
      std::string name,
      std::optional<uint64_t> expectedVersion,
      std::string value) override {
    thrift::CasRefRequest request;
    request.name() = std::move(name);
    if (expectedVersion) {
      request.expectedVersion() = static_cast<int64_t>(*expectedVersion);
    }
    request.value() = std::move(value);
    auto client = connection_.client();
    co_return fromThrift(
        co_await connection_.translated(client, client->co_casRef(request)));
  }

  folly::coro::Task<std::vector<Ref>> co_listRefs(
      std::string prefix,
      std::string startAfter,
      uint32_t limit) override {
    auto client = connection_.client();
    co_return fromThrift(co_await connection_.translated(
        client,
        client->co_listRefs(prefix, startAfter, static_cast<int32_t>(limit))));
  }

  folly::coro::Task<bool> co_removeRef(
      std::string name,
      std::optional<uint64_t> expectedVersion) override {
    thrift::RemoveRefRequest request;
    request.name() = std::move(name);
    if (expectedVersion) {
      request.expectedVersion() = static_cast<int64_t>(*expectedVersion);
    }
    auto client = connection_.client();
    co_return co_await connection_.translated(
        client, client->co_removeRef(request));
  }

  folly::coro::Task<void> co_ping() {
    const std::string message = "ping";
    auto client = connection_.client();
    co_await connection_.translated(client, client->co_ping(message));
  }

 private:
  Connection<thrift::MetadataService> connection_;
};

class RemoteStorage : public StorageRpc {
 public:
  explicit RemoteStorage(const Endpoint& endpoint)
      : connection_{endpoint} {}

  folly::coro::Task<std::unique_ptr<folly::IOBuf>>
  co_read(RegionId region, uint64_t offset, uint64_t length) override {
    auto client = connection_.client();
    auto data = co_await connection_.translated(
        client,
        client->co_read(
            static_cast<int64_t>(region),
            static_cast<int64_t>(offset),
            static_cast<int64_t>(length)));
    co_return std::make_unique<folly::IOBuf>(std::move(data));
  }

  folly::coro::Task<void> co_write(
      RegionId region,
      uint64_t offset,
      const folly::IOBuf& data) override {
    auto client = connection_.client();
    co_await connection_.translated(
        client,
        client->co_write(
            static_cast<int64_t>(region), static_cast<int64_t>(offset), data));
  }

 private:
  Connection<thrift::StorageService> connection_;
};

} // namespace

std::shared_ptr<MetadataRpc> connectMetadata(const Endpoint& endpoint) {
  return std::make_shared<RemoteMetadata>(endpoint);
}

std::shared_ptr<StorageRpc> connectStorage(const Endpoint& endpoint) {
  return std::make_shared<RemoteStorage>(endpoint);
}

folly::coro::Task<void> co_pingMetadata(const Endpoint& endpoint) {
  RemoteMetadata metadata(endpoint);
  co_await metadata.co_ping();
}

} // namespace kiln::rpc
