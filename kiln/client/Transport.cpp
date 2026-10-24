#include "kiln/client/Transport.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <mutex>
#include <unordered_map>

#include <fmt/format.h>
#include <folly/io/IOBuf.h>

#include "kiln/common/Error.h"
#include "kiln/rpc/Rpc.h"

namespace kiln::client {

namespace {

class TcpTransport : public Transport {
 public:
  bool canReach(const RegionInfo& region) const override {
    return region.tcp.has_value();
  }

  folly::coro::Task<void> co_read(
      const RegionInfo& region,
      uint64_t offset,
      folly::MutableByteRange dst) override {
    auto data =
        co_await storage(region).co_read(region.id, offset, dst.size());
    if (data->computeChainDataLength() != dst.size()) {
      throw Error(
          ErrorCode::kInternal,
          fmt::format(
              "Read of {} bytes from {} returned {}",
              dst.size(),
              region.server,
              data->computeChainDataLength()));
    }
    auto* out = dst.data();
    for (auto range : *data) {
      std::memcpy(out, range.data(), range.size());
      out += range.size();
    }
  }

  folly::coro::Task<void> co_write(
      const RegionInfo& region,
      uint64_t offset,
      folly::ByteRange src) override {
    // A copy: fbthrift sends wrapped (unowned) memory more than ten times
    // slower, and a socket may still hold the bytes after a failed call has
    // returned the caller's buffer.
    const auto data = folly::IOBuf::copyBuffer(src);
    co_await storage(region).co_write(region.id, offset, *data);
  }

 private:
  rpc::StorageRpc& storage(const RegionInfo& region) {
    if (!region.tcp) {
      throw Error(
          ErrorCode::kUnavailable,
          fmt::format("Region {} has no TCP access", region.id));
    }
    std::lock_guard lock(mutex_);
    auto& storage = servers_[region.server];
    if (storage == nullptr) {
      storage = rpc::connectStorage(region.tcp->endpoint);
    }
    return *storage;
  }

  std::mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<rpc::StorageRpc>> servers_;
};

class ShmTransport : public Transport {
 public:
  ShmTransport() : host_{localHostName()} {}

  ~ShmTransport() override {
    for (const auto& [id, mapping] : mappings_) {
      ::munmap(mapping.data, mapping.length);
    }
  }

  // The region is on this host, and its file is visible here (it is not if
  // e.g. the server runs in a container with its own /dev/shm).
  bool canReach(const RegionInfo& region) const override {
    if (!region.shm.has_value() || region.shm->host != host_) {
      return false;
    }
    {
      std::lock_guard lock(mutex_);
      if (mappings_.contains(region.id)) {
        return true;
      }
    }
    return ::access(region.shm->path.c_str(), R_OK | W_OK) == 0;
  }

  folly::coro::Task<void> co_read(
      const RegionInfo& region,
      uint64_t offset,
      folly::MutableByteRange dst) override {
    std::memcpy(dst.data(), map(region, offset, dst.size()), dst.size());
    co_return;
  }

  folly::coro::Task<void> co_write(
      const RegionInfo& region,
      uint64_t offset,
      folly::ByteRange src) override {
    std::memcpy(map(region, offset, src.size()), src.data(), src.size());
    co_return;
  }

 private:
  struct Mapping {
    uint8_t* data;
    uint64_t length;
  };

  // The region's memory at 'offset', checked to hold 'length' bytes.
  uint8_t* map(const RegionInfo& region, uint64_t offset, uint64_t length) {
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
    std::lock_guard lock(mutex_);
    auto it = mappings_.find(region.id);
    if (it == mappings_.end()) {
      const auto& path = region.shm->path;
      const int fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
      if (fd < 0) {
        throw Error(
            ErrorCode::kUnavailable,
            fmt::format("Cannot open {}: {}", path, std::strerror(errno)));
      }
      void* data = ::mmap(
          nullptr, region.length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
      const int error = errno;
      ::close(fd);
      if (data == MAP_FAILED) {
        throw Error(
            ErrorCode::kUnavailable,
            fmt::format("Cannot map {}: {}", path, std::strerror(error)));
      }
      it = mappings_
               .emplace(
                   region.id,
                   Mapping{static_cast<uint8_t*>(data), region.length})
               .first;
    }
    return it->second.data + offset;
  }

  const std::string host_;
  mutable std::mutex mutex_;
  std::unordered_map<RegionId, Mapping> mappings_;
};

} // namespace

std::unique_ptr<Transport> makeTcpTransport() {
  return std::make_unique<TcpTransport>();
}

std::unique_ptr<Transport> makeShmTransport() {
  return std::make_unique<ShmTransport>();
}

} // namespace kiln::client
