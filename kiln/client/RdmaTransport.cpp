#include "kiln/client/RdmaTransport.h"

#include <chrono>
#include <cstring>
#include <string>
#include <unordered_map>
#include <vector>

#include <fmt/format.h>
#include <folly/ScopeGuard.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Mutex.h>
#include <folly/fibers/Semaphore.h>
#include <glog/logging.h>

#include "kiln/common/Buffer.h"
#include "kiln/common/Error.h"
#include "rlib/endpoint/RdmaActiveEndpointGroup.h"
#include "rlib/verbs/RdmaError.h"

namespace kiln::client {

std::shared_ptr<void> RdmaMemory::registerMemory(void* data, size_t length) {
  auto region = device_.registerMemory(data, length);
  Key key;
  {
    std::lock_guard lock(mutex_);
    key = Key{reinterpret_cast<uintptr_t>(data), next_++};
    regions_.emplace(key, std::move(region));
  }
  return std::shared_ptr<void>(
      data, [memory = shared_from_this(), key](void*) {
        std::lock_guard lock(memory->mutex_);
        memory->regions_.erase(key);
      });
}

std::optional<RdmaMemory::Found> RdmaMemory::find(
    const void* data,
    size_t length) const {
  const auto start = reinterpret_cast<uintptr_t>(data);
  std::lock_guard lock(mutex_);
  // The registrations that start at or before 'data', nearest first.
  for (auto it = regions_.upper_bound(Key{start, UINT64_MAX});
       it != regions_.begin();) {
    --it;
    const auto offset = start - it->first.first;
    if (offset + length <= it->second.length()) {
      return Found{&it->second, offset};
    }
    // Nested or overlapping registrations are rare: stop at the first.
    break;
  }
  return std::nullopt;
}

namespace {

using namespace std::chrono_literals;

using Group = rlib::RdmaActiveEndpointGroup<rlib::RdmaActiveEndpoint>;
using Factory = rlib::RdmaDefaultEndpointFactory<rlib::RdmaActiveEndpoint>;

// RDMA operations outstanding per connection; more wait.
constexpr int kMaxOutstanding = 64;
// Room in the completion queue for this many connections.
constexpr int kMaxConnections = 64;
constexpr auto kConnectTimeout = 2s;

class RdmaTransport : public Transport {
 public:
  RdmaTransport(
      std::shared_ptr<RdmaMemory> memory,
      std::shared_ptr<RdmaStats> stats)
      : memory_{std::move(memory)}, stats_{std::move(stats)} {
    group_ = std::make_unique<Group>(
        /*polling=*/false,
        kMaxOutstanding,
        /*maxSge=*/1,
        2 * kMaxOutstanding * kMaxConnections);
    // Soft-RoCE drops packets under load; retry like TCP would.
    group_->connParam().retryCount = 7;
    group_->connParam().rnrRetryCount = 7;
    group_->init(std::make_unique<Factory>(*group_));
  }

  ~RdmaTransport() override {
    folly::coro::blockingWait(group_->co_close());
  }

  bool canReach(const RegionInfo& region) const override {
    return region.rdma.has_value();
  }

  folly::coro::Task<void> co_read(
      const RegionInfo& region,
      uint64_t offset,
      folly::MutableByteRange dst) override {
    co_await co_transfer(region, offset, dst.data(), dst.size(), false);
  }

  folly::coro::Task<void> co_write(
      const RegionInfo& region,
      uint64_t offset,
      folly::ByteRange src) override {
    co_await co_transfer(
        region, offset, const_cast<uint8_t*>(src.data()), src.size(), true);
  }

 private:
  struct Connection {
    folly::coro::Mutex mutex;
    std::shared_ptr<rlib::RdmaActiveEndpoint> endpoint;
    folly::fibers::Semaphore slots{kMaxOutstanding};
  };

  // A registered buffer for memory that is not.
  struct Bounce {
    Buffer buffer;
    rlib::MemoryRegion region;
  };

  // Reads into, or writes from, 'length' bytes at 'data'.
  folly::coro::Task<void> co_transfer(
      const RegionInfo& region,
      uint64_t offset,
      uint8_t* data,
      uint64_t length,
      bool write) {
    if (offset > region.length || length > region.length - offset) {
      throw Error(
          ErrorCode::kInvalidArgument,
          fmt::format(
              "Range {}+{} is outside region {} of {} bytes",
              offset,
              length,
              region.id,
              region.length));
    }
    auto connection = connectionTo(*region.rdma);
    auto endpoint = co_await co_endpoint(*connection, *region.rdma);
    co_await connection->slots.co_wait();
    SCOPE_EXIT {
      connection->slots.signal();
    };

    const auto remote = region.rdma->baseAddress + offset;
    const auto rkey = region.rdma->rkey;
    auto rdma = [&](const rlib::MemoryRegion& local, size_t localOffset) {
      return write ? endpoint->co_write(local, localOffset, length, remote, rkey)
                   : endpoint->co_read(local, localOffset, length, remote, rkey);
    };
    int error;
    if (auto found = memory_->find(data, length)) {
      error = co_await rdma(*found->region, found->offset);
      if (error == 0) {
        stats_->direct += length;
      }
    } else {
      auto bounce = takeBounce(length);
      if (write) {
        std::memcpy(bounce->buffer.data(), data, length);
      }
      error = co_await rdma(bounce->region, 0);
      if (!write && error == 0) {
        std::memcpy(data, bounce->buffer.data(), length);
      }
      returnBounce(std::move(bounce));
      countBounced(length);
    }
    if (error != 0) {
      co_await co_drop(*connection, endpoint);
      throw Error(
          ErrorCode::kUnavailable,
          fmt::format(
              "RDMA {} of {} bytes at {} failed: {}",
              write ? "write" : "read",
              length,
              region.rdma->endpoint.toString(),
              std::strerror(error)));
    }
  }

  std::shared_ptr<Connection> connectionTo(const RdmaAccess& access) {
    std::lock_guard lock(mutex_);
    auto& connection = connections_[access.endpoint.toString()];
    if (connection == nullptr) {
      connection = std::make_shared<Connection>();
    }
    return connection;
  }

  // The connection's endpoint, connected now if it is not.
  folly::coro::Task<std::shared_ptr<rlib::RdmaActiveEndpoint>> co_endpoint(
      Connection& connection,
      const RdmaAccess& access) {
    auto lock = co_await connection.mutex.co_scoped_lock();
    if (connection.endpoint == nullptr || connection.endpoint->isClosed()) {
      auto endpoint = group_->createEndpoint();
      std::string failure;
      try {
        co_await endpoint->co_connect(
            rlib::SocketAddress::parse(access.endpoint.toString()),
            kConnectTimeout);
      } catch (const rlib::RdmaError& e) {
        failure = e.what();
      }
      if (!failure.empty()) {
        co_await endpoint->co_close();
        throw Error(
            ErrorCode::kUnavailable,
            fmt::format(
                "Cannot connect to {} with RDMA: {}",
                access.endpoint.toString(),
                failure));
      }
      connection.endpoint = std::move(endpoint);
    }
    co_return connection.endpoint;
  }

  // Closes a failed endpoint, so the next call connects again.
  folly::coro::Task<void> co_drop(
      Connection& connection,
      std::shared_ptr<rlib::RdmaActiveEndpoint> endpoint) {
    {
      auto lock = co_await connection.mutex.co_scoped_lock();
      if (connection.endpoint == endpoint) {
        connection.endpoint.reset();
      }
    }
    co_await endpoint->co_close();
  }

  std::unique_ptr<Bounce> takeBounce(uint64_t length) {
    {
      std::lock_guard lock(mutex_);
      if (length <= kBlockSize && !bounces_.empty()) {
        auto bounce = std::move(bounces_.back());
        bounces_.pop_back();
        return bounce;
      }
    }
    auto buffer = Buffer::allocate(std::max(length, kBlockSize));
    auto region =
        memory_->device().registerMemory(buffer.data(), buffer.capacity());
    return std::unique_ptr<Bounce>(
        new Bounce{std::move(buffer), std::move(region)});
  }

  void returnBounce(std::unique_ptr<Bounce> bounce) {
    std::lock_guard lock(mutex_);
    if (bounce->buffer.capacity() == kBlockSize) {
      bounces_.push_back(std::move(bounce));
    }
  }

  void countBounced(uint64_t length) {
    if (stats_->bounced.fetch_add(length) == 0) {
      LOG(WARNING) << "RDMA from or to memory that is not registered goes "
                   << "through a copy; use Client::allocateBuffer or "
                   << "Client::registerMemory";
    }
  }

  const std::shared_ptr<RdmaMemory> memory_;
  const std::shared_ptr<RdmaStats> stats_;
  std::mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<Connection>> connections_;
  std::vector<std::unique_ptr<Bounce>> bounces_;
  // Declared last, so its endpoints go before the bounce buffers'
  // registrations.
  std::unique_ptr<Group> group_;
};

} // namespace

std::unique_ptr<Transport> makeRdmaTransport(
    std::shared_ptr<RdmaMemory> memory,
    std::shared_ptr<RdmaStats> stats) {
  return std::make_unique<RdmaTransport>(std::move(memory), std::move(stats));
}

} // namespace kiln::client
