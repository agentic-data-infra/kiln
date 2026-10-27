#include "kiln/server/RdmaRegionServer.h"

#include <algorithm>
#include <chrono>

#include <fmt/format.h>
#include <folly/coro/BlockingWait.h>
#include <glog/logging.h>

#include "kiln/common/Error.h"
#include "rlib/endpoint/RdmaPassiveEndpointGroup.h"
#include "rlib/endpoint/RdmaServerEndpoint.h"
#include "rlib/verbs/RdmaError.h"

namespace kiln::server {

RdmaRegionServer::RdmaRegionServer(rdma::Device device)
    : device_{std::move(device)} {}

std::unique_ptr<RdmaRegionServer> RdmaRegionServer::start(
    const std::string& host,
    uint16_t port,
    const std::vector<Memory>& regions) {
  std::unique_ptr<RdmaRegionServer> server(
      new RdmaRegionServer(rdma::Device::open(host)));
  for (const auto& region : regions) {
    server->registrations_.push_back(
        server->device_.registerMemory(region.data, region.length));
  }
  try {
    // Clients post the work requests; the server's QPs get none.
    server->group_ = rlib::RdmaPassiveEndpointGroup<
        rlib::RdmaEndpoint>::createDefaultGroup(/*maxWR=*/1, /*maxSge=*/1, 
                                                /*cqSize=*/2);
    server->listener_ = server->group_->createServerEndpoint();
    server->listener_->bind(
        rlib::SocketAddress::parse(fmt::format("{}:{}", server->device_.host(), port)),
        /*backlog=*/128);
  } catch (const rlib::RdmaError& e) {
    throw Error(
        ErrorCode::kUnavailable,
        fmt::format("Cannot serve RDMA on {}: {}", server->device_.host(), e.what()));
  }
  const Endpoint endpoint{
      .host = server->device_.host(),
      .port = server->listener_->srcAddr().port()};
  for (const auto& registration : server->registrations_) {
    server->access_.push_back(RdmaAccess{
        .endpoint = endpoint,
        .rkey = registration.rkey(),
        .baseAddress = reinterpret_cast<uint64_t>(registration.addr())});
  }
  server->acceptor_ = std::thread([server = server.get()] {
    server->acceptLoop();
  });
  return server;
}

void RdmaRegionServer::acceptLoop() {
  for (;;) {
    std::shared_ptr<rlib::RdmaEndpoint> connection;
    try {
      connection = folly::coro::blockingWait(listener_->co_accept());
    } catch (const rlib::RdmaError& e) {
      std::lock_guard lock(mutex_);
      if (stopping_) {
        return;
      }
      LOG(WARNING) << "Cannot accept an RDMA connection: " << e.what();
      continue;
    }
    std::lock_guard lock(mutex_);
    // Forget the connections that clients closed.
    std::erase_if(connections_, [](const auto& c) { return c->isClosed(); });
    connections_.push_back(std::move(connection));
  }
}

RdmaRegionServer::~RdmaRegionServer() {
  if (acceptor_.joinable()) {
    {
      std::lock_guard lock(mutex_);
      stopping_ = true;
    }
    // Fails the pending accept.
    folly::coro::blockingWait(listener_->co_close());
    acceptor_.join();
  }
  if (group_) {
    // Closes the connections, before their memory's registrations go.
    folly::coro::blockingWait(group_->co_close());
  }
}

} // namespace kiln::server
