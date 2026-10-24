#include "kiln/catalog/Catalog.h"

#include <cstring>

#include <fmt/format.h>

#include "kiln/client/Client.h"
#include "kiln/common/Error.h"

namespace kiln::catalog {

namespace {

// Encoded manifests start with this and their format version.
constexpr char kMagic[4] = {'K', 'M', 'F', 'T'};
constexpr uint32_t kVersion = 1;

class Writer {
 public:
  Writer& u32(uint32_t value) {
    data_.append(reinterpret_cast<const char*>(&value), sizeof(value));
    return *this;
  }

  Writer& u64(uint64_t value) {
    data_.append(reinterpret_cast<const char*>(&value), sizeof(value));
    return *this;
  }

  Writer& string(const std::string& value) {
    u64(value.size());
    data_.append(value);
    return *this;
  }

  std::string& data() {
    return data_;
  }

 private:
  std::string data_;
};

class Reader {
 public:
  explicit Reader(folly::ByteRange data) : data_{data} {}

  uint32_t u32() {
    uint32_t value;
    memcpy(&value, take(sizeof(value)).data(), sizeof(value));
    return value;
  }

  uint64_t u64() {
    uint64_t value;
    memcpy(&value, take(sizeof(value)).data(), sizeof(value));
    return value;
  }

  std::string string() {
    auto bytes = take(u64());
    return std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
  }

  folly::ByteRange take(uint64_t size) {
    if (data_.size() < size) {
      throw Error(ErrorCode::kInvalidArgument, "Manifest is cut short");
    }
    auto result = data_.subpiece(0, size);
    data_.advance(size);
    return result;
  }

  bool done() const {
    return data_.empty();
  }

 private:
  folly::ByteRange data_;
};

} // namespace

std::string encodeManifest(const std::string& key, const Manifest& manifest) {
  Writer writer;
  writer.data().append(kMagic, sizeof(kMagic));
  writer.u32(kVersion)
      .string(key)
      .string(manifest.schema)
      .u64(manifest.rows)
      .u64(manifest.bytes)
      .u64(manifest.chunks.size());
  for (const auto& range : manifest.chunks) {
    writer.u64(range.id.high)
        .u64(range.id.low)
        .u64(range.offset)
        .u32(range.length ? 1 : 0)
        .u64(range.length.value_or(0));
  }
  writer.u64(manifest.properties.size());
  for (const auto& [name, value] : manifest.properties) {
    writer.string(name).string(value);
  }
  return std::move(writer.data());
}

std::pair<std::string, Manifest> decodeManifest(folly::ByteRange data) {
  Reader reader(data);
  if (memcmp(reader.take(sizeof(kMagic)).data(), kMagic, sizeof(kMagic)) != 0) {
    throw Error(ErrorCode::kInvalidArgument, "Not a manifest");
  }
  if (const auto version = reader.u32(); version != kVersion) {
    throw Error(
        ErrorCode::kInvalidArgument,
        fmt::format("Manifest of unknown version {}", version));
  }
  auto key = reader.string();
  Manifest manifest;
  manifest.schema = reader.string();
  manifest.rows = reader.u64();
  manifest.bytes = reader.u64();
  for (auto n = reader.u64(); n > 0; --n) {
    ChunkRange range{.id = {.high = reader.u64(), .low = reader.u64()}};
    range.offset = reader.u64();
    const bool hasLength = reader.u32() != 0;
    const auto length = reader.u64();
    if (hasLength) {
      range.length = length;
    }
    manifest.chunks.push_back(range);
  }
  for (auto n = reader.u64(); n > 0; --n) {
    auto name = reader.string();
    manifest.properties[std::move(name)] = reader.string();
  }
  if (!reader.done()) {
    throw Error(ErrorCode::kInvalidArgument, "Manifest has trailing bytes");
  }
  return {std::move(key), std::move(manifest)};
}

Catalog::Catalog(client::Client& client, CatalogOptions options)
    : client_{client}, options_{std::move(options)} {}

std::string Catalog::refName(const std::string& key) const {
  return options_.refPrefix + ChunkId::of(folly::StringPiece(key)).toString();
}

folly::coro::Task<std::optional<Manifest>> Catalog::co_getMemo(
    std::string key) {
  const auto name = refName(key);
  const auto ref = co_await client_.co_getRef(name);
  if (!ref) {
    co_return std::nullopt;
  }
  // Whether the memo's chunks are gone for good (and not just unavailable).
  std::optional<bool> lost;
  std::optional<Manifest> manifest;
  try {
    const auto id = ChunkId::parse(ref->value);
    const auto size = (co_await client_.co_stat(id)).size;
    auto data = co_await client_.co_read(
        id, 0, size, client_.allocateBuffer(std::max<uint64_t>(size, 1)));
    auto [storedKey, stored] = decodeManifest(data.bytes());
    // Another key with the same hash: not this key's memo.
    if (storedKey != key) {
      co_return std::nullopt;
    }
    manifest = std::move(stored);
    for (const auto& range : manifest->chunks) {
      const auto chunk = co_await client_.co_stat(range.id);
      if (range.offset > chunk.size ||
          range.length.value_or(0) > chunk.size - range.offset) {
        throw Error(
            ErrorCode::kNotFound,
            fmt::format("Chunk {} is too short", range.id.toString()));
      }
    }
  } catch (const Error& e) {
    switch (e.code()) {
      case ErrorCode::kNotFound:
      // Not a manifest, or a range outside its chunk.
      case ErrorCode::kInvalidArgument:
        lost = true;
        break;
      case ErrorCode::kUnavailable:
        lost = false;
        break;
      default:
        throw;
    }
  }
  if (lost == false) {
    co_return std::nullopt;
  }
  if (lost == true) {
    // Only if no one put a new memo in the meantime.
    co_await client_.co_removeRef(name, ref->version);
    co_return std::nullopt;
  }
  co_return manifest;
}

folly::coro::Task<bool> Catalog::co_putMemo(
    std::string key,
    Manifest manifest) {
  auto data = encodeManifest(key, manifest);
  auto writer = co_await client_.co_create(options_.placement);
  auto buffer = client_.registerMemory(data.data(), data.size());
  buffer.setSize(data.size());
  co_await writer->co_append(std::move(buffer));
  const auto id = co_await writer->co_commit();
  // Create-only: an existing memo wins. Its manifest may be this very
  // chunk (same key and manifest), so this one stays.
  const auto ref =
      co_await client_.co_casRef(refName(key), std::nullopt, id.toString());
  co_return ref.has_value();
}

folly::coro::Task<bool> Catalog::co_removeMemo(std::string key) {
  co_return co_await client_.co_removeRef(refName(key));
}

} // namespace kiln::catalog
