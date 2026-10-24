#include "kiln/common/Buffer.h"

#include <new>
#include <stdexcept>

#include <fmt/format.h>

namespace kiln {

namespace {
constexpr size_t kAlignment = 4096;
} // namespace

Buffer Buffer::allocate(size_t capacity) {
  // aligned_alloc wants a multiple of the alignment.
  const auto bytes = (capacity + kAlignment - 1) / kAlignment * kAlignment;
  auto* data = static_cast<uint8_t*>(
      std::aligned_alloc(kAlignment, bytes == 0 ? kAlignment : bytes));
  if (data == nullptr) {
    throw std::bad_alloc();
  }
  Buffer buffer;
  buffer.owned_.reset(data);
  buffer.data_ = data;
  buffer.capacity_ = capacity;
  return buffer;
}

Buffer Buffer::wrap(void* data, size_t capacity) {
  Buffer buffer;
  buffer.data_ = static_cast<uint8_t*>(data);
  buffer.capacity_ = capacity;
  return buffer;
}

void Buffer::setSize(size_t size) {
  if (size > capacity_) {
    throw std::out_of_range(fmt::format(
        "Buffer size {} exceeds its capacity {}", size, capacity_));
  }
  size_ = size;
}

} // namespace kiln
