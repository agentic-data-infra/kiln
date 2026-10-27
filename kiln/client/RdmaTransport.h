// Internal to kiln/client, built only with -c kiln.rdma=true: one-sided RDMA
// to and from storage servers' DRAM regions (RdmaAccess), straight between
// them and the caller's registered buffers.

#pragma once

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>

#include "kiln/client/Transport.h"
#include "kiln/rdma/Device.h"

namespace kiln::client {

// The memory a client registered with its RDMA device, found by address.
class RdmaMemory : public std::enable_shared_from_this<RdmaMemory> {
 public:
  explicit RdmaMemory(rdma::Device device) : device_{std::move(device)} {}

  // Registers 'length' bytes at 'data' until the returned handle goes.
  // Throws Error(kUnavailable) if the device refuses.
  std::shared_ptr<void> registerMemory(void* data, size_t length);

  struct Found {
    const rlib::MemoryRegion* region;
    // Of the range in the registration.
    size_t offset;
  };

  // The registration that covers 'length' bytes at 'data', if any. It
  // stays valid while the memory's handle does.
  std::optional<Found> find(const void* data, size_t length) const;

  const rdma::Device& device() const {
    return device_;
  }

 private:
  // A registration's start address, and a number for overlapping ones.
  using Key = std::pair<uintptr_t, uint64_t>;

  const rdma::Device device_;
  mutable std::mutex mutex_;
  std::map<Key, rlib::MemoryRegion> regions_;
  uint64_t next_{0};
};

struct RdmaStats {
  // Bytes moved straight from and to registered caller memory.
  std::atomic<uint64_t> direct{0};
  // Bytes copied through bounce buffers, as the caller's memory was not
  // registered.
  std::atomic<uint64_t> bounced{0};
};

// RDMA to the regions that have RdmaAccess, one connection per storage
// server, made on first use and again after a failure. Memory that 'memory'
// does not cover goes through registered bounce buffers, counted in 'stats'.
std::unique_ptr<Transport> makeRdmaTransport(
    std::shared_ptr<RdmaMemory> memory,
    std::shared_ptr<RdmaStats> stats);

} // namespace kiln::client
