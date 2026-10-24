#pragma once

#include <cstdint>
#include <memory>

#include <folly/coro/Task.h>
#include <folly/io/IOBuf.h>

#include "kiln/common/Types.h"

namespace kiln::rpc {

// A storage server's operations on its regions, independent of fbthrift. The
// storage server implements them (kiln/server), and connectStorage (Rpc.h)
// calls them over the network. Calls fail with kiln::Error, with
// kInvalidArgument for a range outside the region.
class StorageRpc {
 public:
  virtual ~StorageRpc() = default;

  virtual folly::coro::Task<std::unique_ptr<folly::IOBuf>>
  co_read(RegionId region, uint64_t offset, uint64_t length) = 0;

  virtual folly::coro::Task<void>
  co_write(RegionId region, uint64_t offset, const folly::IOBuf& data) = 0;
};

} // namespace kiln::rpc
