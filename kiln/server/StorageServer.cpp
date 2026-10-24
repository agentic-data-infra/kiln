#include "kiln/server/StorageServer.h"

#include <cstdlib>
#include <filesystem>
#include <map>

#include <sstream>

#include <fmt/format.h>
#include <folly/FileUtil.h>
#include <folly/coro/Baton.h>
#include <folly/coro/BlockingWait.h>
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

// A kept directory's file of region ids: lines of "<file index> <id>".
constexpr const char* kIdsFile = "region-ids";

std::map<uint32_t, RegionId> readIds(const std::string& dir) {
  std::map<uint32_t, RegionId> ids;
  std::string data;
  if (!folly::readFile((dir + "/" + kIdsFile).c_str(), data)) {
    return ids;
  }
  std::istringstream lines(data);
  uint32_t index;
  RegionId id;
  while (lines >> index >> id) {
    ids[index] = id;
  }
  return ids;
}

void writeIds(const std::string& dir, const std::vector<RegionInfo>& regions) {
  std::string data;
  for (size_t i = 0; i < regions.size(); ++i) {
    data += fmt::format("{} {}\n", i, regions[i].id);
  }
  iovec iov{.iov_base = data.data(), .iov_len = data.size()};
  if (folly::writeFileAtomicNoThrow(
          dir + "/" + kIdsFile, &iov, 1, 0644, folly::SyncType::WITH_SYNC) !=
      0) {
    LOG(WARNING) << "Cannot write the region ids in " << dir
                 << "; the regions will not be reattached";
  }
}

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
  server->options_ = options;
  server->keep_ = options.tier == Tier::kLocalFs && !options.dir.empty();
  std::map<uint32_t, RegionId> ids;
  uint32_t count = options.regions;
  if (server->keep_) {
    server->dir_ = options.dir;
    std::error_code error;
    std::filesystem::create_directories(server->dir_, error);
    if (error) {
      throw Error(
          ErrorCode::kUnavailable,
          fmt::format("Cannot create {}: {}", server->dir_, error.message()));
    }
    ids = readIds(server->dir_);
    // Keep the regions there are, even beyond options.regions.
    while (std::filesystem::exists(
        fmt::format("{}/region-{}", server->dir_, count))) {
      ++count;
    }
  } else {
    server->dir_ = makeDirectory(options);
  }

  std::vector<std::unique_ptr<Region>> regions;
  std::vector<RegionInfo> infos;
  for (uint32_t i = 0; i < count; ++i) {
    regions.push_back(std::make_unique<Region>(
        options.tier,
        fmt::format("{}/region-{}", server->dir_, i),
        options.regionSize,
        server->keep_));
    auto id = ids.find(i);
    infos.push_back(RegionInfo{
        .id = id == ids.end() ? 0 : id->second,
        .tier = regions.back()->tier(),
        .length = regions.back()->length(),
        .shm = ShmAccess{
            .host = localHostName(), .path = regions.back()->path()}});
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
  server->metadata_ = rpc::connectMetadata(options.metadata);
  server->regions_ = co_await server->metadata_->co_registerServer(
      std::move(infos), options.budget);
  if (server->keep_) {
    writeIds(server->dir_, server->regions_);
  }
  server->store_->registered(server->regions_);
  server->reregister_ = std::thread([server = server.get()] {
    server->reregisterLoop();
  });
  LOG(INFO) << "Storage server " << server->endpoint_.toString() << " serves "
            << count << " " << toString(options.tier)
            << " regions of " << options.regionSize << " bytes in "
            << server->dir_
            << (options.budget == 0
                    ? std::string()
                    : fmt::format(", {} bytes of them in use at most", options.budget));
  co_return server;
}

void StorageServer::reregisterLoop() {
  bool failed = false;
  std::unique_lock lock(mutex_);
  while (!stop_.wait_for(lock, options_.registerInterval, [&] {
    return stopping_;
  })) {
    lock.unlock();
    try {
      // The same ids come back.
      folly::coro::blockingWait(
          metadata_->co_registerServer(regions_, options_.budget));
      if (failed) {
        LOG(INFO) << "Registered again with " << options_.metadata.toString();
      }
      failed = false;
    } catch (const Error& e) {
      if (!failed) {
        LOG(WARNING) << "Cannot register with "
                     << options_.metadata.toString() << ": " << e.what();
      }
      failed = true;
    }
    lock.lock();
  }
}

StorageServer::~StorageServer() {
  if (reregister_.joinable()) {
    {
      std::lock_guard lock(mutex_);
      stopping_ = true;
    }
    stop_.notify_all();
    reregister_.join();
  }
  rpcServer_.reset();
  store_.reset();
  if (!keep_) {
    std::error_code error;
    std::filesystem::remove_all(dir_, error);
  }
}

} // namespace kiln::server
