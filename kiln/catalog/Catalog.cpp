#include "kiln/catalog/Catalog.h"

#include <cstring>

#include <fmt/format.h>
#include <folly/ScopeGuard.h>

#include "kiln/client/BufferPool.h"
#include "kiln/client/Client.h"
#include "kiln/common/Error.h"

namespace kiln::catalog {

namespace {

// Encoded manifests start with this and their format version.
constexpr char kMagic[4] = {'K', 'M', 'F', 'T'};
// Version 1 had no entry names, kinds, rows nor bucket.
constexpr uint32_t kVersion = 2;

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
      .u64(manifest.bucket)
      .u64(manifest.chunks.size());
  for (const auto& range : manifest.chunks) {
    writer.u32(static_cast<uint32_t>(range.kind))
        .string(range.name)
        .u64(range.id.high)
        .u64(range.id.low)
        .u64(range.offset)
        .u32(range.length ? 1 : 0)
        .u64(range.length.value_or(0))
        .u32(range.rows ? 1 : 0)
        .u64(range.rows.value_or(0));
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
  const auto version = reader.u32();
  if (version != 1 && version != kVersion) {
    throw Error(
        ErrorCode::kInvalidArgument,
        fmt::format("Manifest of unknown version {}", version));
  }
  auto key = reader.string();
  Manifest manifest;
  manifest.schema = reader.string();
  manifest.rows = reader.u64();
  manifest.bytes = reader.u64();
  if (version >= 2) {
    manifest.bucket = reader.u64();
  }
  // Reads a flag and a value: the value if the flag is set.
  auto optional = [&]() -> std::optional<uint64_t> {
    const bool present = reader.u32() != 0;
    const auto value = reader.u64();
    return present ? std::optional<uint64_t>(value) : std::nullopt;
  };
  for (auto n = reader.u64(); n > 0; --n) {
    ChunkRange range;
    if (version >= 2) {
      const auto kind = reader.u32();
      if (kind > static_cast<uint32_t>(EntryKind::kManifest)) {
        throw Error(
            ErrorCode::kInvalidArgument,
            fmt::format("Manifest entry of unknown kind {}", kind));
      }
      range.kind = static_cast<EntryKind>(kind);
      range.name = reader.string();
    }
    range.id.high = reader.u64();
    range.id.low = reader.u64();
    range.offset = reader.u64();
    range.length = optional();
    if (version >= 2) {
      range.rows = optional();
    }
    manifest.chunks.push_back(std::move(range));
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
    auto [storedKey, stored] = co_await co_readManifest(id);
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

folly::coro::Task<ChunkId> Catalog::co_writeManifest(
    std::string key,
    Manifest manifest) {
  auto data = encodeManifest(key, manifest);
  auto writer = co_await client_.co_create(kDefaultBucket, options_.placement);
  const auto& pool = options_.buffers;
  const auto n = pool == nullptr
      ? 0
      : (data.size() + pool->bufferSize() - 1) / pool->bufferSize();
  if (n == 0 || n > pool->count()) {
    auto buffer = client_.registerMemory(data.data(), data.size());
    buffer.setSize(data.size());
    co_await writer->co_append(std::move(buffer));
  } else {
    auto buffers = co_await pool->co_acquire(n);
    SCOPE_EXIT {
      pool->release(std::move(buffers));
    };
    for (size_t i = 0; i < n; ++i) {
      const auto offset = i * pool->bufferSize();
      const auto length = std::min(data.size() - offset, pool->bufferSize());
      std::memcpy(buffers[i].data(), data.data() + offset, length);
      buffers[i].setSize(length);
    }
    buffers = co_await writer->co_append(std::move(buffers));
  }
  co_return co_await writer->co_commit();
}

folly::coro::Task<std::pair<std::string, Manifest>> Catalog::co_readManifest(
    ChunkId id) {
  const auto size = (co_await client_.co_stat(id)).size;
  const auto& pool = options_.buffers;
  if (pool == nullptr) {
    auto data = co_await client_.co_read(
        id, 0, size, client_.allocateBuffer(std::max<uint64_t>(size, 1)));
    co_return decodeManifest(data.bytes());
  }
  auto buffers = co_await pool->co_acquire(1);
  SCOPE_EXIT {
    pool->release(std::move(buffers));
  };
  if (size <= pool->bufferSize()) {
    buffers[0] = co_await client_.co_read(id, 0, size, std::move(buffers[0]));
    co_return decodeManifest(buffers[0].bytes());
  }
  // Larger than a buffer: in pieces, copied together.
  std::string data;
  data.reserve(size);
  for (uint64_t done = 0; done < size;) {
    const auto length = std::min<uint64_t>(size - done, pool->bufferSize());
    buffers[0] =
        co_await client_.co_read(id, done, length, std::move(buffers[0]));
    if (buffers[0].size() != length) {
      throw Error(
          ErrorCode::kInvalidArgument,
          fmt::format("Manifest {} is cut short", id.toString()));
    }
    data.append(
        reinterpret_cast<const char*>(buffers[0].data()), buffers[0].size());
    done += length;
  }
  co_return decodeManifest(folly::ByteRange(
      reinterpret_cast<const uint8_t*>(data.data()), data.size()));
}

folly::coro::Task<bool> Catalog::co_putMemo(
    std::string key,
    Manifest manifest) {
  const auto id = co_await co_writeManifest(key, std::move(manifest));
  // Create-only: an existing memo wins. Its manifest may be this very
  // chunk (same key and manifest), so this one stays.
  const auto ref =
      co_await client_.co_casRef(refName(key), std::nullopt, id.toString());
  co_return ref.has_value();
}

folly::coro::Task<bool> Catalog::co_removeMemo(std::string key) {
  co_return co_await client_.co_removeRef(refName(key));
}

std::string Catalog::datasetRefName(const std::string& name) const {
  auto refName = options_.datasetRefPrefix + name;
  if (name.empty() || name.find('/') != std::string::npos ||
      refName.size() > kMaxRefNameSize) {
    throw Error(
        ErrorCode::kInvalidArgument,
        fmt::format("Bad dataset name '{}'", name));
  }
  return refName;
}

folly::coro::Task<std::optional<Dataset>> Catalog::co_getDataset(
    std::string name) {
  const auto refName = datasetRefName(name);
  const auto ref = co_await client_.co_getRef(refName);
  if (!ref) {
    co_return std::nullopt;
  }
  const auto id = ChunkId::parse(ref->value);
  auto [storedName, manifest] = co_await co_readManifest(id);
  if (storedName != name) {
    throw Error(
        ErrorCode::kInvalidArgument,
        fmt::format("Ref {} points at the manifest of {}", refName, storedName));
  }
  Dataset dataset{
      .name = std::move(name),
      .version = ref->version,
      .manifestId = id,
      .manifest = std::move(manifest)};
  co_return dataset;
}

folly::coro::Task<std::optional<uint64_t>> Catalog::co_publishDataset(
    std::string name,
    Manifest manifest,
    std::optional<uint64_t> expectedVersion) {
  const auto refName = datasetRefName(name);
  const auto id = co_await co_writeManifest(name, std::move(manifest));
  const auto ref =
      co_await client_.co_casRef(refName, expectedVersion, id.toString());
  if (!ref) {
    // Unless the current version is this very manifest.
    const auto current = co_await client_.co_getRef(refName);
    if (!current || current->value != id.toString()) {
      co_await client_.co_remove(id);
    }
    co_return std::nullopt;
  }
  co_return ref->version;
}

folly::coro::Task<std::vector<std::string>> Catalog::co_listDatasets(
    std::string prefix) {
  std::vector<std::string> names;
  const auto refPrefix = options_.datasetRefPrefix + prefix;
  for (const auto& ref : co_await client_.co_listRefs(refPrefix)) {
    names.push_back(ref.name.substr(options_.datasetRefPrefix.size()));
  }
  co_return names;
}

folly::coro::Task<bool> Catalog::co_deleteDataset(std::string name) {
  for (;;) {
    auto dataset = co_await co_getDataset(name);
    if (!dataset) {
      co_return false;
    }
    // Only this version: a new one published meanwhile is read again.
    const auto refName = datasetRefName(name);
    if (!co_await client_.co_removeRef(refName, dataset->version)) {
      continue;
    }
    if (dataset->manifest.bucket != kDefaultBucket) {
      co_await client_.co_deleteBucket(dataset->manifest.bucket);
    }
    co_await client_.co_remove(dataset->manifestId);
    co_return true;
  }
}

} // namespace kiln::catalog
