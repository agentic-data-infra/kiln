#pragma once

#include <folly/coro/Task.h>

namespace kiln::rpc {

// The metadata server's operations as Kiln's client sees them, independent
// of fbthrift. Every call is a folly::coro::Task. Empty for now: the
// operations come with the metadata server (kiln/meta).
class MetadataRpc {
 public:
  virtual ~MetadataRpc() = default;
};

} // namespace kiln::rpc
