// Kiln's RDMA support, on rlib; built only with -c kiln.rdma=true.

#pragma once

#include <infiniband/verbs.h>

#include <cstddef>
#include <memory>
#include <string>

#include "rlib/verbs/MemoryRegion.h"

namespace rlib {
class RdmaCmId;
class RdmaEventChannel;
}

namespace kiln::rdma {

// The RDMA device (NIC) of a local address, and its protection domain: the
// one rlib's endpoints on the device share, so memory registered here, before
// any endpoint exists, works with all of them. Move only.
class Device {
 public:
  // The device of the local interface 'host' (an address or a host name),
  // or, if empty, of the first interface that has one. Throws
  // Error(kUnavailable) if there is none.
  static Device open(const std::string& host = "");

  Device(Device&&) noexcept;
  Device& operator=(Device&&) noexcept;
  ~Device();

  // E.g. "rxe0".
  std::string name() const;

  // The address of the interface the device was found by.
  const std::string& host() const {
    return host_;
  }

  ibv_context* context() const;

  ibv_pd* pd() const {
    return pd_;
  }

  // Registers 'length' bytes at 'data' for local writes and remote reads and
  // writes, pinning them until the region is destroyed. Throws
  // Error(kUnavailable) if the device refuses, e.g. beyond the locked memory
  // limit (ulimit -l).
  rlib::MemoryRegion registerMemory(void* data, size_t length) const;

 private:
  Device(
      std::unique_ptr<rlib::RdmaEventChannel> channel,
      std::unique_ptr<rlib::RdmaCmId> id,
      std::string host);

  // An id bound to the address, which holds the device open; destroyed
  // before its channel.
  std::unique_ptr<rlib::RdmaEventChannel> channel_;
  std::unique_ptr<rlib::RdmaCmId> id_;
  std::string host_;
  ibv_pd* pd_{nullptr};
};

} // namespace kiln::rdma
