#include "kiln/common/ChunkId.h"

#include <fmt/format.h>
#include <folly/io/IOBuf.h>

#define XXH_INLINE_ALL
#include <xxhash.h>

namespace kiln {

namespace {

ChunkId toChunkId(XXH128_hash_t hash) {
  return ChunkId{.high = hash.high64, .low = hash.low64};
}

} // namespace

ChunkId ChunkId::of(folly::ByteRange data) {
  return toChunkId(XXH3_128bits(data.data(), data.size()));
}

ChunkId ChunkId::of(const folly::IOBuf& data) {
  XXH3_state_t state;
  XXH3_128bits_reset(&state);
  for (auto range : data) {
    XXH3_128bits_update(&state, range.data(), range.size());
  }
  return toChunkId(XXH3_128bits_digest(&state));
}

std::string ChunkId::toString() const {
  return fmt::format("{:016x}{:016x}", high, low);
}

} // namespace kiln
