// Built only with -c kiln.rdma=true.

#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "kiln/common/Types.h"
#include "kiln/rdma/Device.h"

namespace rlib {
class RdmaEndpoint;
template <typename C>
class RdmaPassiveEndpointGroup;
template <typename C>
class RdmaServerEndpoint;
}

namespace kiln::server {

// Serves a storage server's DRAM regions for one-sided RDMA: registers their
// memory with the NIC and accepts connections, after which clients read and
// write the regions with no work for this server's CPU.
class RdmaRegionServer {
 public:
  struct Memory {
    uint8_t* data;
    uint64_t length;
  };

  // Registers 'regions' with the device of 'host' (empty for the first
  // interface that has one) and listens there on 'port' (0 for any). Throws
  // Error(kUnavailable) if it cannot.
  static std::unique_ptr<RdmaRegionServer>
  start(const std::string& host, uint16_t port, const std::vector<Memory>& regions);

  // Stops accepting and closes the connections; then the regions'
  // registrations go.
  ~RdmaRegionServer();

  // How clients reach the i'th region.
  const RdmaAccess& access(size_t i) const {
    return access_.at(i);
  }

 private:
  explicit RdmaRegionServer(rdma::Device device);

  // Accepts connections until stopped.
  void acceptLoop();

  rdma::Device device_;
  std::vector<rlib::MemoryRegion> registrations_;
  std::vector<RdmaAccess> access_;
  std::unique_ptr<rlib::RdmaPassiveEndpointGroup<rlib::RdmaEndpoint>> group_;
  std::shared_ptr<rlib::RdmaServerEndpoint<rlib::RdmaEndpoint>> listener_;
  std::thread acceptor_;
  std::mutex mutex_;
  bool stopping_{false};
  // The accepted connections; nothing is posted on them.
  std::vector<std::shared_ptr<rlib::RdmaEndpoint>> connections_;
};

} // namespace kiln::server
