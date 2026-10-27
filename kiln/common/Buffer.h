#pragma once

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>

#include <folly/Range.h>

namespace kiln {

// Contiguous memory that chunk data is written from and read into: 'size'
// bytes of data in 'capacity' bytes of memory. Clients get one from
// Client::allocateBuffer or wrap their own memory with
// Client::registerMemory; with the RDMA transport both are registered with
// the NIC, so that chunk data moves straight between them and the storage
// servers. Move only.
class Buffer {
 public:
  Buffer() = default;

  // 'capacity' bytes of page-aligned memory, owned by the buffer.
  static Buffer allocate(size_t capacity);

  // 'capacity' bytes at 'data', which the caller owns and keeps alive while
  // the buffer is in use.
  static Buffer wrap(void* data, size_t capacity);

  Buffer(Buffer&&) noexcept = default;
  Buffer& operator=(Buffer&& other) noexcept;

  // Keeps 'registration' (e.g. of the memory with the NIC) as long as the
  // buffer, and releases it before the memory.
  void setRegistration(std::shared_ptr<void> registration) {
    registration_ = std::move(registration);
  }

  uint8_t* data() {
    return data_;
  }

  const uint8_t* data() const {
    return data_;
  }

  size_t size() const {
    return size_;
  }

  size_t capacity() const {
    return capacity_;
  }

  // Throws std::out_of_range if 'size' exceeds the capacity.
  void setSize(size_t size);

  folly::ByteRange bytes() const {
    return {data_, size_};
  }

 private:
  struct Free {
    void operator()(uint8_t* data) const {
      std::free(data);
    }
  };

  std::unique_ptr<uint8_t, Free> owned_;
  // Declared after owned_, so destroyed before it.
  std::shared_ptr<void> registration_;
  uint8_t* data_{nullptr};
  size_t size_{0};
  size_t capacity_{0};
};

} // namespace kiln
