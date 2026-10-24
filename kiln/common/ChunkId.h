#pragma once

#include <cstdint>
#include <functional>
#include <string>

#include <folly/Range.h>

namespace folly {
class IOBuf;
} // namespace folly

namespace kiln {

// A chunk's name: the 128-bit xxHash3 of its content. Equal content has the
// same id, so a chunk is written once however many refs point at it.
struct ChunkId {
  uint64_t high{0};
  uint64_t low{0};

  // The id of 'data'.
  static ChunkId of(folly::ByteRange data);

  // The id of the bytes of an IOBuf chain, as if they were contiguous.
  static ChunkId of(const folly::IOBuf& data);

  // 32 lowercase hex digits, high word first.
  std::string toString() const;

  bool operator==(const ChunkId&) const = default;
  auto operator<=>(const ChunkId&) const = default;
};

} // namespace kiln

template <>
struct std::hash<kiln::ChunkId> {
  size_t operator()(const kiln::ChunkId& id) const {
    // The id is a hash already.
    return id.low;
  }
};
