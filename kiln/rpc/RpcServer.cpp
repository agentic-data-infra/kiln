#include <fmt/format.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include <thrift/lib/cpp2/util/ScopedServerInterfaceThread.h>

#include "kiln/rpc/Convert.h"
#include "kiln/rpc/Rpc.h"
#include "kiln/rpc/gen-cpp2/MetadataService.h"
#include "kiln/rpc/gen-cpp2/StorageService.h"

namespace kiln::rpc {

namespace {

// Awaits a handler's call, turning kiln::Error into the exception clients
// turn back into it.
template <typename T>
folly::coro::Task<T> guarded(folly::coro::Task<T> call) {
  try {
    co_return co_await std::move(call);
  } catch (const Error& e) {
    throw toThrift(e);
  }
}

template <typename T>
std::unique_ptr<T> boxed(T value) {
  return std::make_unique<T>(std::move(value));
}

// The generated services, answered by Kiln's implementations.

class MetadataService
    : public apache::thrift::ServiceHandler<thrift::MetadataService> {
 public:
  explicit MetadataService(std::shared_ptr<MetadataRpc> metadata)
      : metadata_{std::move(metadata)} {}

  folly::coro::Task<std::unique_ptr<std::string>> co_ping(
      std::unique_ptr<std::string> message) override {
    co_return std::move(message);
  }

  folly::coro::Task<std::unique_ptr<std::vector<thrift::RegionInfo>>>
  co_registerServer(
      std::unique_ptr<std::vector<thrift::RegionInfo>> regions,
      int64_t budget) override {
    if (budget < 0) {
      throw toThrift(Error(ErrorCode::kInvalidArgument, "Negative budget"));
    }
    auto result = co_await guarded(metadata_->co_registerServer(
        fromThrift(*regions), static_cast<uint64_t>(budget)));
    co_return boxed(toThrift(result));
  }

  folly::coro::Task<std::unique_ptr<std::vector<thrift::RegionInfo>>>
  co_getRegions(std::unique_ptr<std::vector<int64_t>> ids) override {
    std::vector<RegionId> regionIds(ids->begin(), ids->end());
    auto result =
        co_await guarded(metadata_->co_getRegions(std::move(regionIds)));
    co_return boxed(toThrift(result));
  }

  folly::coro::Task<std::unique_ptr<std::vector<thrift::BlockRef>>>
  co_allocate(thrift::Tier tier, int32_t count) override {
    if (count < 0) {
      throw toThrift(Error(ErrorCode::kInvalidArgument, "Negative count"));
    }
    auto result = co_await guarded(metadata_->co_allocate(
        static_cast<Tier>(tier), static_cast<uint32_t>(count)));
    co_return boxed(toThrift(result));
  }

  folly::coro::Task<void> co_free(
      std::unique_ptr<std::vector<thrift::BlockRef>> blocks) override {
    co_await guarded(metadata_->co_free(fromThrift(*blocks)));
  }

  folly::coro::Task<std::unique_ptr<thrift::ChunkInfo>> co_commit(
      std::unique_ptr<thrift::ChunkInfo> chunk) override {
    auto result = co_await guarded(metadata_->co_commit(fromThrift(*chunk)));
    co_return boxed(toThrift(result));
  }

  folly::coro::Task<std::unique_ptr<thrift::ChunkInfo>> co_lookup(
      std::unique_ptr<thrift::ChunkId> id) override {
    auto result = co_await guarded(metadata_->co_lookup(fromThrift(*id)));
    co_return boxed(toThrift(result));
  }

  folly::coro::Task<bool> co_remove(
      std::unique_ptr<thrift::ChunkId> id) override {
    co_return co_await guarded(metadata_->co_remove(fromThrift(*id)));
  }

  folly::coro::Task<std::unique_ptr<thrift::RefLookup>> co_getRef(
      std::unique_ptr<std::string> name) override {
    auto result = co_await guarded(metadata_->co_getRef(std::move(*name)));
    co_return boxed(toThrift(result));
  }

  folly::coro::Task<std::unique_ptr<thrift::RefLookup>> co_casRef(
      std::unique_ptr<thrift::CasRefRequest> request) override {
    auto result = co_await guarded(metadata_->co_casRef(
        std::move(*request->name()),
        versionOf(request->expectedVersion()),
        std::move(*request->value())));
    co_return boxed(toThrift(result));
  }

  folly::coro::Task<std::unique_ptr<std::vector<thrift::Ref>>> co_listRefs(
      std::unique_ptr<std::string> prefix,
      std::unique_ptr<std::string> startAfter,
      int32_t limit) override {
    if (limit < 0) {
      throw toThrift(Error(ErrorCode::kInvalidArgument, "Negative limit"));
    }
    auto result = co_await guarded(metadata_->co_listRefs(
        std::move(*prefix),
        std::move(*startAfter),
        static_cast<uint32_t>(limit)));
    co_return boxed(toThrift(result));
  }

  folly::coro::Task<bool> co_removeRef(
      std::unique_ptr<thrift::RemoveRefRequest> request) override {
    co_return co_await guarded(metadata_->co_removeRef(
        std::move(*request->name()), versionOf(request->expectedVersion())));
  }

 private:
  template <typename Field>
  static std::optional<uint64_t> versionOf(Field field) {
    if (!field) {
      return std::nullopt;
    }
    return static_cast<uint64_t>(*field);
  }

  const std::shared_ptr<MetadataRpc> metadata_;
};

class StorageService
    : public apache::thrift::ServiceHandler<thrift::StorageService> {
 public:
  explicit StorageService(std::shared_ptr<StorageRpc> storage)
      : storage_{std::move(storage)} {}

  folly::coro::Task<std::unique_ptr<folly::IOBuf>>
  co_read(int64_t region, int64_t offset, int64_t length) override {
    if (offset < 0 || length < 0) {
      throw toThrift(Error(ErrorCode::kInvalidArgument, "Negative range"));
    }
    co_return co_await guarded(storage_->co_read(
        static_cast<RegionId>(region),
        static_cast<uint64_t>(offset),
        static_cast<uint64_t>(length)));
  }

  folly::coro::Task<void> co_write(
      int64_t region,
      int64_t offset,
      std::unique_ptr<folly::IOBuf> data) override {
    if (offset < 0) {
      throw toThrift(Error(ErrorCode::kInvalidArgument, "Negative offset"));
    }
    co_await guarded(storage_->co_write(
        static_cast<RegionId>(region), static_cast<uint64_t>(offset), *data));
  }

 private:
  const std::shared_ptr<StorageRpc> storage_;
};

std::unique_ptr<apache::thrift::ScopedServerInterfaceThread> serve(
    std::shared_ptr<apache::thrift::AsyncProcessorFactory> service,
    const std::string& host,
    uint16_t port) {
  // "::" accepts IPv4 connections too.
  const auto bindHost = host.empty() ? std::string("::") : host;
  try {
    return std::make_unique<apache::thrift::ScopedServerInterfaceThread>(
        std::move(service),
        bindHost,
        port,
        [](apache::thrift::ThriftServer& server) {
          // Clients keep their connections between calls.
          server.setIdleTimeout(std::chrono::milliseconds(0));
          server.setWorkersJoinTimeout(std::chrono::seconds(1));
        });
  } catch (const std::exception& e) {
    throw Error(
        ErrorCode::kUnavailable,
        fmt::format(
            "Cannot serve on {}: {}",
            Endpoint{bindHost, port}.toString(),
            e.what()));
  }
}

} // namespace

RpcServer::RpcServer(
    std::unique_ptr<apache::thrift::ScopedServerInterfaceThread> server)
    : server_{std::move(server)} {}

RpcServer::~RpcServer() = default;

uint16_t RpcServer::port() const {
  return server_->getPort();
}

std::unique_ptr<RpcServer> serveMetadata(
    std::shared_ptr<MetadataRpc> metadata,
    const std::string& host,
    uint16_t port) {
  return std::unique_ptr<RpcServer>(new RpcServer(serve(
      std::make_shared<MetadataService>(std::move(metadata)), host, port)));
}

std::unique_ptr<RpcServer> serveStorage(
    std::shared_ptr<StorageRpc> storage,
    const std::string& host,
    uint16_t port) {
  return std::unique_ptr<RpcServer>(new RpcServer(serve(
      std::make_shared<StorageService>(std::move(storage)), host, port)));
}

} // namespace kiln::rpc
