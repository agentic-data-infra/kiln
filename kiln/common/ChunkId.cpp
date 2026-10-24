#include "kiln/common/ChunkId.h"

#include <fmt/format.h>
#include <folly/io/IOBuf.h>

#include "kiln/common/Error.h"

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
  ChunkHasher hasher;
  for (auto range : data) {
    hasher.update(range);
  }
  return hasher.id();
}

ChunkId ChunkId::parse(const std::string& id) {
  auto word = [&](size_t start) {
    uint64_t value = 0;
    for (size_t i = start; i < start + 16; ++i) {
      const char c = id[i];
      int digit;
      if (c >= '0' && c <= '9') {
        digit = c - '0';
      } else if (c >= 'a' && c <= 'f') {
        digit = c - 'a' + 10;
      } else {
        throw Error(
            ErrorCode::kInvalidArgument,
            fmt::format("Bad chunk id '{}'", id));
      }
      value = (value << 4) | digit;
    }
    return value;
  };
  if (id.size() != 32) {
    throw Error(
        ErrorCode::kInvalidArgument,
        fmt::format("Bad chunk id '{}': expected 32 hex digits", id));
  }
  return ChunkId{.high = word(0), .low = word(16)};
}

struct ChunkHasher::State {
  XXH3_state_t xxh;
};

ChunkHasher::ChunkHasher() : state_{std::make_unique<State>()} {
  XXH3_128bits_reset(&state_->xxh);
}

ChunkHasher::~ChunkHasher() = default;

void ChunkHasher::update(folly::ByteRange data) {
  XXH3_128bits_update(&state_->xxh, data.data(), data.size());
}

ChunkId ChunkHasher::id() const {
  return toChunkId(XXH3_128bits_digest(&state_->xxh));
}

std::string ChunkId::toString() const {
  return fmt::format("{:016x}{:016x}", high, low);
}

} // namespace kiln
