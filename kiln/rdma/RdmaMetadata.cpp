#include "kiln/rdma/RdmaMetadata.h"

#include <chrono>

#include <fmt/format.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Mutex.h>
#include <glog/logging.h>

#include "kiln/rdma/Device.h"
#include "kiln/rdma/MetadataProtocol.h"
#include "rlib/rpc/RdmaRpcClientGroup.h"
#include "rlib/rpc/RdmaRpcServerGroup.h"
#include "rlib/verbs/RdmaError.h"

namespace kiln::rdma {

namespace {

using namespace std::chrono_literals;

constexpr auto kConnectTimeout = 2s;

// Calls in flight per connection.
constexpr int kQueueDepth = 16;

// Calls 'metadata' as 'request' asks, and returns the response.
folly::coro::Task<std::string> co_dispatch(
    rpc::MetadataRpc& metadata,
    const std::string& request) {
  MessageReader in(request);
  auto out = okResponse();
  switch (static_cast<MetadataOp>(in.u8())) {
    case MetadataOp::kRegisterServer: {
      auto regions = in.regions();
      const auto budget = in.u64();
      out.regions(
          co_await metadata.co_registerServer(std::move(regions), budget));
      break;
    }
    case MetadataOp::kGetRegions:
      out.regions(co_await metadata.co_getRegions(in.regionIds()));
      break;
    case MetadataOp::kAllocate: {
      const auto tier = static_cast<Tier>(in.u8());
      const auto count = in.u32();
      std::optional<Tier> overflow;
      if (in.u8() != 0) {
        overflow = static_cast<Tier>(in.u8());
      }
      out.blocks(co_await metadata.co_allocate(tier, count, overflow));
      break;
    }
    case MetadataOp::kFree:
      co_await metadata.co_free(in.blocks());
      break;
    case MetadataOp::kCommit:
      out.chunk(co_await metadata.co_commit(in.chunk()));
      break;
    case MetadataOp::kLookup: {
      ChunkId id;
      id.high = in.u64();
      id.low = in.u64();
      out.chunk(co_await metadata.co_lookup(id));
      break;
    }
    case MetadataOp::kRemove: {
      ChunkId id;
      id.high = in.u64();
      id.low = in.u64();
      out.u8(co_await metadata.co_remove(id));
      break;
    }
    case MetadataOp::kGetRef:
      out.optionalRef(co_await metadata.co_getRef(in.string()));
      break;
    case MetadataOp::kCasRef: {
      auto name = in.string();
      const auto expected = in.optionalVersion();
      auto value = in.string();
      out.optionalRef(co_await metadata.co_casRef(
          std::move(name), expected, std::move(value)));
      break;
    }
    case MetadataOp::kListRefs: {
      auto prefix = in.string();
      auto startAfter = in.string();
      const auto limit = in.u32();
      out.refs(co_await metadata.co_listRefs(
          std::move(prefix), std::move(startAfter), limit));
      break;
    }
    case MetadataOp::kRemoveRef: {
      auto name = in.string();
      const auto expected = in.optionalVersion();
      out.u8(co_await metadata.co_removeRef(std::move(name), expected));
      break;
    }
    default:
      throw Error(ErrorCode::kInvalidArgument, "Unknown metadata call");
  }
  co_return std::move(out).finish("The response");
}

} // namespace

// Answers the calls of all connections with the metadata server's state.
class MetadataService : public rlib::RdmaRpcService<MetadataProtocol> {
 public:
  explicit MetadataService(std::shared_ptr<rpc::MetadataRpc> metadata)
      : metadata_{std::move(metadata)} {}

  folly::coro::Task<void> co_process(
      rlib::RdmaRpcServerEvent<MetadataProtocol>& event) override {
    std::optional<Error> error;
    try {
      event.response().bytes =
          co_await co_dispatch(*metadata_, event.request().bytes);
    } catch (const Error& e) {
      error = e;
    } catch (const std::exception& e) {
      error = Error(ErrorCode::kInternal, e.what());
    }
    if (error) {
      event.response().bytes = errorResponse(*error);
    }
  }


 private:
  const std::shared_ptr<rpc::MetadataRpc> metadata_;
};

struct RdmaMetadataServer::State {
  explicit State(std::shared_ptr<rpc::MetadataRpc> metadata)
      : service{std::move(metadata)} {}

  MetadataService service;
  // Declared after the service, so destroyed before it.
  std::unique_ptr<rlib::RdmaRpcServerGroup<MetadataProtocol>> group;
};

std::unique_ptr<RdmaMetadataServer> RdmaMetadataServer::start(
    std::shared_ptr<rpc::MetadataRpc> metadata,
    const std::string& host,
    uint16_t port) {
  const auto device = Device::open(host);
  std::unique_ptr<RdmaMetadataServer> server(new RdmaMetadataServer());
  server->state_ = std::make_unique<State>(std::move(metadata));
  rlib::RdmaRpcServerOptions options;
  options.recvQueue = kQueueDepth;
  try {
    auto& group = server->state_->group;
    group = rlib::RdmaRpcServerGroup<MetadataProtocol>::create(
        server->state_->service, options);
    group->serve(rlib::SocketAddress::parse(
        fmt::format("{}:{}", device.host(), port)));
    server->endpoint_ =
        Endpoint{.host = device.host(), .port = group->address().port()};
  } catch (const rlib::RdmaError& e) {
    throw Error(
        ErrorCode::kUnavailable,
        fmt::format(
            "Cannot serve metadata over RDMA on {}: {}",
            device.host(),
            e.what()));
  }
  return server;
}

RdmaMetadataServer::~RdmaMetadataServer() {
  if (state_ && state_->group) {
    folly::coro::blockingWait(state_->group->co_close());
  }
}

namespace {

using ClientGroup = rlib::RdmaRpcClientGroup<MetadataProtocol>;
using ClientEndpoint = ClientGroup::Endpoint;

class RdmaMetadataClient : public rpc::MetadataRpc {
 public:
  explicit RdmaMetadataClient(Endpoint endpoint)
      : endpoint_{std::move(endpoint)} {
    rlib::RdmaRpcOptions options;
    options.recvQueue = kQueueDepth;
    group_ = ClientGroup::create(options);
    // Soft-RoCE drops packets under load; retry like TCP would.
    group_->connParam().retryCount = 7;
    group_->connParam().rnrRetryCount = 7;
  }

  ~RdmaMetadataClient() override {
    folly::coro::blockingWait(group_->co_close());
  }

  folly::coro::Task<std::vector<RegionInfo>> co_registerServer(
      std::vector<RegionInfo> regions,
      uint64_t budget) override {
    MessageWriter request;
    request.u8(static_cast<uint8_t>(MetadataOp::kRegisterServer))
        .regions(regions)
        .u64(budget);
    const auto response = co_await co_call(std::move(request));
    co_return readResponse(response).regions();
  }

  folly::coro::Task<std::vector<RegionInfo>> co_getRegions(
      std::vector<RegionId> ids) override {
    MessageWriter request;
    request.u8(static_cast<uint8_t>(MetadataOp::kGetRegions)).regionIds(ids);
    const auto response = co_await co_call(std::move(request));
    co_return readResponse(response).regions();
  }

  folly::coro::Task<std::vector<BlockRef>> co_allocate(
      Tier tier,
      uint32_t count,
      std::optional<Tier> overflow) override {
    MessageWriter request;
    request.u8(static_cast<uint8_t>(MetadataOp::kAllocate))
        .u8(static_cast<uint8_t>(tier))
        .u32(count)
        .u8(overflow ? 1 : 0);
    if (overflow) {
      request.u8(static_cast<uint8_t>(*overflow));
    }
    const auto response = co_await co_call(std::move(request));
    co_return readResponse(response).blocks();
  }

  folly::coro::Task<void> co_free(std::vector<BlockRef> blocks) override {
    MessageWriter request;
    request.u8(static_cast<uint8_t>(MetadataOp::kFree)).blocks(blocks);
    const auto response = co_await co_call(std::move(request));
    readResponse(response);
  }

  folly::coro::Task<ChunkInfo> co_commit(ChunkInfo chunk) override {
    MessageWriter request;
    request.u8(static_cast<uint8_t>(MetadataOp::kCommit)).chunk(chunk);
    const auto response = co_await co_call(std::move(request));
    co_return readResponse(response).chunk();
  }

  folly::coro::Task<ChunkInfo> co_lookup(ChunkId id) override {
    MessageWriter request;
    request.u8(static_cast<uint8_t>(MetadataOp::kLookup))
        .u64(id.high)
        .u64(id.low);
    const auto response = co_await co_call(std::move(request));
    co_return readResponse(response).chunk();
  }

  folly::coro::Task<bool> co_remove(ChunkId id) override {
    MessageWriter request;
    request.u8(static_cast<uint8_t>(MetadataOp::kRemove))
        .u64(id.high)
        .u64(id.low);
    const auto response = co_await co_call(std::move(request));
    co_return readResponse(response).u8() != 0;
  }

  folly::coro::Task<std::optional<Ref>> co_getRef(std::string name) override {
    MessageWriter request;
    request.u8(static_cast<uint8_t>(MetadataOp::kGetRef)).string(name);
    const auto response = co_await co_call(std::move(request));
    co_return readResponse(response).optionalRef();
  }

  folly::coro::Task<std::optional<Ref>> co_casRef(
      std::string name,
      std::optional<uint64_t> expectedVersion,
      std::string value) override {
    MessageWriter request;
    request.u8(static_cast<uint8_t>(MetadataOp::kCasRef))
        .string(name)
        .optionalVersion(expectedVersion)
        .string(value);
    const auto response = co_await co_call(std::move(request));
    co_return readResponse(response).optionalRef();
  }

  folly::coro::Task<std::vector<Ref>> co_listRefs(
      std::string prefix,
      std::string startAfter,
      uint32_t limit) override {
    MessageWriter request;
    request.u8(static_cast<uint8_t>(MetadataOp::kListRefs))
        .string(prefix)
        .string(startAfter)
        .u32(limit);
    const auto response = co_await co_call(std::move(request));
    co_return readResponse(response).refs();
  }

  folly::coro::Task<bool> co_removeRef(
      std::string name,
      std::optional<uint64_t> expectedVersion) override {
    MessageWriter request;
    request.u8(static_cast<uint8_t>(MetadataOp::kRemoveRef))
        .string(name)
        .optionalVersion(expectedVersion);
    const auto response = co_await co_call(std::move(request));
    co_return readResponse(response).u8() != 0;
  }

 private:
  // Sends 'request' and returns the response's bytes.
  folly::coro::Task<std::string> co_call(MessageWriter request) {
    MetadataMessage message{std::move(request).finish("The request")};
    auto endpoint = co_await co_endpoint();
    MetadataMessage response;
    std::string failure;
    try {
      co_await endpoint->co_call(message, response);
    } catch (const rlib::RdmaError& e) {
      failure = e.what();
    }
    if (!failure.empty()) {
      co_await co_drop(endpoint);
      throw Error(
          ErrorCode::kUnavailable,
          fmt::format(
              "RDMA metadata call to {} failed: {}",
              endpoint_.toString(),
              failure));
    }
    co_return std::move(response.bytes);
  }

  // The endpoint, connected now if it is not.
  folly::coro::Task<std::shared_ptr<ClientEndpoint>> co_endpoint() {
    auto lock = co_await mutex_.co_scoped_lock();
    if (connected_ == nullptr || connected_->isClosed()) {
      auto endpoint = group_->createEndpoint();
      std::string failure;
      try {
        co_await endpoint->co_connect(
            rlib::SocketAddress::parse(endpoint_.toString()), kConnectTimeout);
      } catch (const rlib::RdmaError& e) {
        failure = e.what();
      }
      if (!failure.empty()) {
        co_await endpoint->co_close();
        throw Error(
            ErrorCode::kUnavailable,
            fmt::format(
                "Cannot connect to the metadata server at {} with RDMA: {}",
                endpoint_.toString(),
                failure));
      }
      connected_ = std::move(endpoint);
    }
    co_return connected_;
  }

  // Closes a failed endpoint, so that the next call connects again.
  folly::coro::Task<void> co_drop(std::shared_ptr<ClientEndpoint> endpoint) {
    {
      auto lock = co_await mutex_.co_scoped_lock();
      if (connected_ == endpoint) {
        connected_.reset();
      }
    }
    co_await endpoint->co_close();
  }

  const Endpoint endpoint_;
  std::unique_ptr<ClientGroup> group_;
  folly::coro::Mutex mutex_;
  std::shared_ptr<ClientEndpoint> connected_;
};

} // namespace

std::shared_ptr<rpc::MetadataRpc> connectRdmaMetadata(const Endpoint& endpoint) {
  return std::make_shared<RdmaMetadataClient>(endpoint);
}

} // namespace kiln::rdma
