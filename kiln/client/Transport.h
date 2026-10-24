// Internal to kiln/client: how a client moves bytes to and from a storage
// server's region.

#pragma once

#include <cstdint>
#include <memory>

#include <folly/Range.h>
#include <folly/coro/Task.h>

#include "kiln/common/Types.h"

namespace kiln::client {

class Transport {
 public:
  virtual ~Transport() = default;

  // Whether this transport can reach 'region' from here.
  virtual bool canReach(const RegionInfo& region) const = 0;

  // Reads dst.size() bytes at 'offset' in the region into 'dst'.
  virtual folly::coro::Task<void> co_read(
      const RegionInfo& region,
      uint64_t offset,
      folly::MutableByteRange dst) = 0;

  // Writes 'src' at 'offset' in the region.
  virtual folly::coro::Task<void> co_write(
      const RegionInfo& region,
      uint64_t offset,
      folly::ByteRange src) = 0;
};

// The storage servers' TCP service (TcpAccess), one connection per server.
std::unique_ptr<Transport> makeTcpTransport();

// Regions on this host, by mapping their files (ShmAccess). Each region is
// mapped once, on first use, until the transport is destroyed.
std::unique_ptr<Transport> makeShmTransport();

} // namespace kiln::client
