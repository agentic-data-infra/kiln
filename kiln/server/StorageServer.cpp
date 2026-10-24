#include "kiln/server/StorageServer.h"

#include <cstdlib>
#include <filesystem>
#include <map>

#include <fmt/format.h>
#include <folly/coro/Baton.h>
#include <folly/coro/Sleep.h>
#include <glog/logging.h>

#include "kiln/common/Error.h"
#include "kiln/rpc/Rpc.h"
#include "kiln/server/Region.h"

namespace kiln::server {

// The regions behind the storage service. Their ids come from the metadata
// server, so calls wait until registration is done.
class RegionStore : public rpc::StorageRpc {
 public:
  explicit RegionStore(std::vector<std::unique_ptr<Region>> regions)
      : unregistered_{std::move(regions)} {}

  void registered(const std::vector<RegionInfo>& infos) {
    for (size_t i = 0; i < infos.size(); ++i) {
      regions_.emplace(infos[i].id, std::move(unregistered_[i]));
    }
    unregistered_.clear();
    registered_.post();
  }

  folly::coro::Task<std::unique_ptr<folly::IOBuf>>
  co_read(RegionId region, uint64_t offset, uint64_t length) override {
    co_await registered_;
    co_return find(region).read(offset, length);
  }

  folly::coro::Task<void> co_write(
      RegionId region,
      uint64_t offset,
      const folly::IOBuf& data) override {
    co_await registered_;
    find(region).write(offset, data);
  }

 private:
  Region& find(RegionId id) {
    auto it = regions_.find(id);
    if (it == regions_.end()) {
      throw Error(
          ErrorCode::kInvalidArgument,
          fmt::format("No region {} on this server", id));
    }
    return *it->second;
  }

  std::vector<std::unique_ptr<Region>> unregistered_;
  // Written once, before registered_ is posted.
  std::map<RegionId, std::unique_ptr<Region>> regions_;
  folly::coro::Baton registered_;
};

namespace {

std::string makeDirectory(const StorageServerOptions& options) {
  std::string parent = options.dir;
  if (parent.empty()) {
    const char* tmp = std::getenv("TMPDIR");
    parent = options.tier == Tier::kDram ? "/dev/shm"
        : tmp != nullptr                 ? tmp
                                         : "/tmp";
  }
  auto pattern = (std::filesystem::path(parent) / "kiln-XXXXXX").string();
  if (::mkdtemp(pattern.data()) == nullptr) {
    throw Error(
        ErrorCode::kUnavailable,
        fmt::format("Cannot create a directory in {}", parent));
  }
  return pattern;
}

// Waits until the metadata server answers.
folly::coro::Task<void> co_waitForMetadata(
    const Endpoint& metadata,
    std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  for (;;) {
    try {
      co_await rpc::co_pingMetadata(metadata);
      co_return;
    } catch (const Error& e) {
      if (e.code() != ErrorCode::kUnavailable ||
          std::chrono::steady_clock::now() >= deadline) {
        throw;
      }
    }
    co_await folly::coro::sleep(std::chrono::milliseconds(200));
  }
}

} // namespace

folly::coro::Task<std::unique_ptr<StorageServer>> StorageServer::co_start(
    StorageServerOptions options) {
  if (options.regionSize == 0 || options.regionSize % kBlockSize != 0) {
    throw Error(
        ErrorCode::kInvalidArgument,
        fmt::format(
            "Region size {} is not a multiple of the block size {}",
            options.regionSize,
            kBlockSize));
  }
  std::unique_ptr<StorageServer> server(new StorageServer());
  server->dir_ = makeDirectory(options);

  std::vector<std::unique_ptr<Region>> regions;
  for (uint32_t i = 0; i < options.regions; ++i) {
    regions.push_back(std::make_unique<Region>(
        options.tier,
        fmt::format("{}/region-{}", server->dir_, i),
        options.regionSize));
  }
  std::vector<RegionInfo> infos;
  for (const auto& region : regions) {
    infos.push_back(RegionInfo{
        .tier = region->tier(),
        .length = region->length(),
        .shm = ShmAccess{.host = localHostName(), .path = region->path()}});
  }

  server->store_ = std::make_shared<RegionStore>(std::move(regions));
  server->rpcServer_ =
      rpc::serveStorage(server->store_, options.bindHost, options.port);
  server->endpoint_ = Endpoint{
      .host = options.host.empty() ? localHostName() : options.host,
      .port = server->rpcServer_->port()};
  for (auto& info : infos) {
    info.server = server->endpoint_.toString();
    info.tcp = TcpAccess{.endpoint = server->endpoint_};
  }

  co_await co_waitForMetadata(options.metadata, options.connectTimeout);
  auto metadata = rpc::connectMetadata(options.metadata);
  server->regions_ = co_await metadata->co_registerServer(std::move(infos));
  server->store_->registered(server->regions_);
  LOG(INFO) << "Storage server " << server->endpoint_.toString() << " serves "
            << options.regions << " " << toString(options.tier)
            << " regions of " << options.regionSize << " bytes in "
            << server->dir_;
  co_return server;
}

StorageServer::~StorageServer() {
  rpcServer_.reset();
  store_.reset();
  std::error_code error;
  std::filesystem::remove_all(dir_, error);
}

} // namespace kiln::server
