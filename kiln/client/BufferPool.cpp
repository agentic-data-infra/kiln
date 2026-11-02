#include "kiln/client/BufferPool.h"

#include <fmt/format.h>

#include "kiln/client/Client.h"
#include "kiln/common/Error.h"

namespace kiln::client {

BufferPool::BufferPool(Client& client, size_t bufferSize, size_t count)
    : client_{client}, bufferSize_{bufferSize}, count_{count} {
  free_.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    free_.push_back(client_.allocateBuffer(bufferSize_));
  }
}

folly::coro::Task<std::vector<Buffer>> BufferPool::co_acquire(size_t n) {
  if (n > count_) {
    throw Error(
        ErrorCode::kInvalidArgument,
        fmt::format("Asked for {} of a pool of {} buffers", n, count_));
  }
  Waiter waiter{.n = n};
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (waiters_.empty() && free_.size() >= n) {
      co_return take(n);
    }
    waiters_.push_back(&waiter);
  }
  co_await waiter.baton;
  co_return std::move(waiter.buffers);
}

std::vector<Buffer> BufferPool::tryAcquire(size_t n) {
  std::lock_guard<std::mutex> l(mutex_);
  if (!waiters_.empty() || free_.size() < n) {
    return {};
  }
  return take(n);
}

void BufferPool::release(Buffer buffer) {
  std::vector<Buffer> buffers;
  buffers.push_back(std::move(buffer));
  release(std::move(buffers));
}

void BufferPool::release(std::vector<Buffer> buffers) {
  for (auto& buffer : buffers) {
    if (buffer.capacity() == 0) {
      buffer = client_.allocateBuffer(bufferSize_);
    }
  }
  std::vector<Waiter*> ready;
  {
    std::lock_guard<std::mutex> l(mutex_);
    for (auto& buffer : buffers) {
      free_.push_back(std::move(buffer));
    }
    while (!waiters_.empty() && free_.size() >= waiters_.front()->n) {
      auto* waiter = waiters_.front();
      waiters_.pop_front();
      waiter->buffers = take(waiter->n);
      ready.push_back(waiter);
    }
  }
  for (auto* waiter : ready) {
    waiter->baton.post();
  }
}

size_t BufferPool::available() const {
  std::lock_guard<std::mutex> l(mutex_);
  return free_.size();
}

std::vector<Buffer> BufferPool::take(size_t n) {
  std::vector<Buffer> taken;
  for (size_t i = 0; i < n; ++i) {
    taken.push_back(std::move(free_.back()));
    free_.pop_back();
    taken.back().setSize(0);
  }
  return taken;
}

} // namespace kiln::client
