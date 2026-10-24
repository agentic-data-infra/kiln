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

// Awaits a generated client's call, turning its failures into kiln::Error.
template <typename T>
folly::coro::Task<T> translated(
    folly::coro::Task<T> call,
    const Endpoint& endpoint) {
  try {
    co_return co_await std::move(call);
  } catch (const thrift::KilnException& e) {
    throw fromThrift(e);
  } catch (const apache::thrift::transport::TTransportException& e) {
    throw Error(
        ErrorCode::kUnavailable,
        fmt::format("Cannot reach {}: {}", endpoint.toString(), e.what()));
  } catch (const Error&) {
    throw;
  } catch (const std::exception& e) {
    throw Error(
        ErrorCode::kInternal,
        fmt::format("Call to {} failed: {}", endpoint.toString(), e.what()));
  }
}

class RemoteMetadata : public MetadataRpc {
 public:
  explicit RemoteMetadata(const Endpoint& endpoint)
      : endpoint_{endpoint},
        client_{makeClient<thrift::MetadataService>(endpoint)} {}

  folly::coro::Task<std::vector<RegionInfo>> co_registerServer(
      std::vector<RegionInfo> regions,
      uint64_t budget) override {
    const auto request = toThrift(regions);
    co_return fromThrift(co_await translated(
        client_->co_registerServer(request, static_cast<int64_t>(budget)),
        endpoint_));
  }

  folly::coro::Task<std::vector<RegionInfo>> co_getRegions(
      std::vector<RegionId> ids) override {
    const std::vector<int64_t> request(ids.begin(), ids.end());
    co_return fromThrift(
        co_await translated(client_->co_getRegions(request), endpoint_));
  }

  folly::coro::Task<std::vector<BlockRef>> co_allocate(
      Placement placement,
      uint32_t count) override {
    const auto request = toThrift(placement);
    co_return fromThrift(co_await translated(
        client_->co_allocate(request, static_cast<int32_t>(count)),
        endpoint_));
  }

  folly::coro::Task<void> co_free(std::vector<BlockRef> blocks) override {
    const auto request = toThrift(blocks);
    co_await translated(client_->co_free(request), endpoint_);
  }

  folly::coro::Task<ChunkInfo> co_commit(ChunkInfo chunk) override {
    const auto request = toThrift(chunk);
    co_return fromThrift(
        co_await translated(client_->co_commit(request), endpoint_));
  }

  folly::coro::Task<ChunkInfo> co_lookup(ChunkId id) override {
    const auto request = toThrift(id);
    co_return fromThrift(
        co_await translated(client_->co_lookup(request), endpoint_));
  }

  folly::coro::Task<bool> co_remove(ChunkId id) override {
    const auto request = toThrift(id);
    co_return co_await translated(client_->co_remove(request), endpoint_);
  }

  folly::coro::Task<void> co_ping() {
    const std::string message = "ping";
    co_await translated(client_->co_ping(message), endpoint_);
  }

 private:
  const Endpoint endpoint_;
  const std::unique_ptr<apache::thrift::Client<thrift::MetadataService>>
      client_;
};

class RemoteStorage : public StorageRpc {
 public:
  explicit RemoteStorage(const Endpoint& endpoint)
      : endpoint_{endpoint},
        client_{makeClient<thrift::StorageService>(endpoint)} {}

  folly::coro::Task<std::unique_ptr<folly::IOBuf>>
  co_read(RegionId region, uint64_t offset, uint64_t length) override {
    auto data = co_await translated(
        client_->co_read(
            static_cast<int64_t>(region),
            static_cast<int64_t>(offset),
            static_cast<int64_t>(length)),
        endpoint_);
    co_return std::make_unique<folly::IOBuf>(std::move(data));
  }

  folly::coro::Task<void> co_write(
      RegionId region,
      uint64_t offset,
      const folly::IOBuf& data) override {
    co_await translated(
        client_->co_write(
            static_cast<int64_t>(region), static_cast<int64_t>(offset), data),
        endpoint_);
  }

 private:
  const Endpoint endpoint_;
  const std::unique_ptr<apache::thrift::Client<thrift::StorageService>>
      client_;
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
