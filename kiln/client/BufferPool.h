#pragma once

#include <cstddef>
#include <deque>
#include <mutex>
#include <vector>

#include <folly/coro/Baton.h>
#include <folly/coro/Task.h>

#include "kiln/common/Buffer.h"

namespace kiln::client {

class Client;

// A fixed number of equal buffers from Client::allocateBuffer, allocated up
// front and reused, so that moving chunk data never waits for memory to be
// allocated or, with RDMA, registered with the NIC. Without RDMA they are
// plain memory. Callers take buffers, fill or read into them, and give them
// back; when none are free they wait. Thread safe.
class BufferPool {
 public:
  // 'count' buffers of 'bufferSize' bytes each, allocated now. The client
  // must outlive the pool.
  BufferPool(Client& client, size_t bufferSize, size_t count);

  BufferPool(const BufferPool&) = delete;
  BufferPool& operator=(const BufferPool&) = delete;

  size_t bufferSize() const {
    return bufferSize_;
  }

  size_t count() const {
    return count_;
  }

  // 'n' empty buffers, taken together once that many are free, so that a
  // caller never holds some while it waits for more. Waiting callers are
  // served in order. Fails with kInvalidArgument for more than count().
  folly::coro::Task<std::vector<Buffer>> co_acquire(size_t n = 1);

  // 'n' empty buffers if that many are free and nobody waits, else none.
  std::vector<Buffer> tryAcquire(size_t n = 1);

  // Gives back a buffer taken from this pool. An empty one, e.g. lost to a
  // failed call, is replaced with a new buffer.
  void release(Buffer buffer);

  void release(std::vector<Buffer> buffers);

  // Buffers free now.
  size_t available() const;

 private:
  struct Waiter {
    size_t n;
    std::vector<Buffer> buffers;
    folly::coro::Baton baton;
  };

  // Takes 'n' free buffers. Holds mutex_.
  std::vector<Buffer> take(size_t n);

  Client& client_;
  const size_t bufferSize_;
  const size_t count_;
  mutable std::mutex mutex_;
  std::vector<Buffer> free_;
  std::deque<Waiter*> waiters_;
};

} // namespace kiln::client
