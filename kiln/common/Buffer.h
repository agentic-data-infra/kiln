#pragma once

#include <memory>

#include <folly/io/IOBuf.h>

namespace kiln {

// Bytes moving between clients, servers and storage tiers. A placeholder for
// now: an IOBuf chain, which fbthrift sends and receives without copying.
// Registered (RDMA) memory will come behind the same name.
using Buffer = std::unique_ptr<folly::IOBuf>;

} // namespace kiln
