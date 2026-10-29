#include "kiln/client/Client.h"

#include <algorithm>

#include <fmt/format.h>
#include <folly/coro/Collect.h>
#include <folly/lang/Bits.h>
#include <glog/logging.h>

#include "kiln/client/Transport.h"
#include "kiln/common/Error.h"
#include "kiln/objectstore/ObjectStore.h"
#include "kiln/rpc/Rpc.h"
#if KILN_RDMA
#include "kiln/client/RdmaTransport.h"
#include "kiln/rdma/RdmaMetadata.h"
#endif

namespace kiln::client {

namespace {

// Forgets cached chunk infos past this many.
constexpr size_t kMaxCachedChunks = 1 << 16;

// Refs listed per call: at most kMaxRefNameSize + kMaxRefValueSize bytes
// each, so a page fits an RDMA metadata message.
constexpr uint32_t kRefsPerPage = 32;

// Regions asked for per call, for the same reason.
constexpr size_t kRegionsPerCall = 64;

} // namespace

Client::Client(ClientOptions options)
    : options_{std::move(options)},
      tcp_{makeTcpTransport()},
      shm_{makeShmTransport()} {
  const bool rdmaMetadata = options_.metadata.rdma && !options_.metaForceTcp;
#if KILN_RDMA
  // RDMA is usable if there is a device.
  std::optional<rdma::Device> device;
  if (rdmaMetadata || !options_.dataForceTcp) {
    try {
      device = rdma::Device::open(options_.rdmaHost);
    } catch (const Error& e) {
      if (rdmaMetadata) {
        LOG(WARNING) << e.what() << "; metadata calls over TCP";
      }
    }
  }
  if (device && rdmaMetadata) {
    metadata_ = rdma::connectRdmaMetadata(*options_.metadata.rdma);
    rdmaMetadata_ = true;
  }
  if (device && !options_.dataForceTcp) {
    rdmaMemory_ = std::make_shared<RdmaMemory>(std::move(*device));
    rdmaStats_ = std::make_shared<RdmaStats>();
    rdma_ = makeRdmaTransport(rdmaMemory_, rdmaStats_);
  }
#else
  if (rdmaMetadata) {
    LOG(WARNING) << "Built without RDMA (-c kiln.rdma=true); metadata calls "
                 << "over TCP";
  }
#endif
  if (metadata_ == nullptr) {
    if (!options_.metadata.tcp) {
      throw Error(
          ErrorCode::kInvalidArgument,
          fmt::format(
              "No metadata URL this client can use in '{}'",
              options_.metadata.toString()));
    }
    metadata_ = rpc::connectMetadata(*options_.metadata.tcp);
  }
}

Client::~Client() = default;

Buffer Client::allocateBuffer(size_t capacity) {
  auto buffer = Buffer::allocate(capacity);
  registerWithNic(buffer);
  return buffer;
}

Buffer Client::registerMemory(void* data, size_t length) {
  auto buffer = Buffer::wrap(data, length);
  registerWithNic(buffer);
  return buffer;
}

void Client::registerWithNic(Buffer& buffer) {
#if KILN_RDMA
  if (rdmaMemory_ == nullptr || buffer.capacity() == 0) {
    return;
  }
  try {
    buffer.setRegistration(
        rdmaMemory_->registerMemory(buffer.data(), buffer.capacity()));
  } catch (const Error& e) {
    // Still usable, through a copy.
    LOG_FIRST_N(WARNING, 1) << e.what();
  }
#endif
}

ClientStats Client::stats() const {
  ClientStats stats;
  stats.rdmaMetadata = rdmaMetadata_;
  stats.rdmaData = rdma_ != nullptr;
#if KILN_RDMA
  if (rdmaStats_) {
    stats.rdmaBytes = rdmaStats_->direct;
    stats.rdmaCopiedBytes = rdmaStats_->bounced;
  }
#endif
  return stats;
}

folly::coro::Task<std::unique_ptr<ChunkWriter>> Client::co_create(
    BucketId bucket,
    Placement placement,
    uint64_t sizeHint) {
  if (!hasBlocks(placement.tier) || placement.writeThrough == placement.tier) {
    throw Error(
        ErrorCode::kInvalidArgument,
        fmt::format("Cannot place a chunk in {}", toString(placement.tier)));
  }
  if (placement.writeThrough && !isDurable(*placement.writeThrough)) {
    throw Error(
        ErrorCode::kInvalidArgument,
        fmt::format(
            "Cannot write through to {}, which is not durable",
            toString(*placement.writeThrough)));
  }
  if (placement.overflow &&
      (!hasBlocks(*placement.overflow) ||
       placement.overflow == placement.tier || placement.writeThrough)) {
    throw Error(
        ErrorCode::kInvalidArgument,
        fmt::format(
            "Cannot overflow from {} to {}{}",
            toString(placement.tier),
            toString(*placement.overflow),
            placement.writeThrough ? " while writing through" : ""));
  }
  if (placement.writeThrough == Tier::kObjectStore && !options_.objectStore) {
    throw Error(
        ErrorCode::kInvalidArgument,
        "Writing through to the object store needs one in ClientOptions");
  }
  co_return std::unique_ptr<ChunkWriter>(
      new ChunkWriter(*this, bucket, placement, sizeHint));
}

folly::coro::Task<Buffer>
Client::co_read(ChunkId id, uint64_t offset, uint64_t length, Buffer&& dst) {
  const auto chunk = co_await co_stat(id);
  if (offset > chunk.size) {
    throw Error(
        ErrorCode::kInvalidArgument,
        fmt::format(
            "Offset {} is past the end of chunk {} of {} bytes",
            offset,
            id.toString(),
            chunk.size));
  }
  length = std::min(length, chunk.size - offset);
  if (dst.capacity() < length) {
    throw Error(
        ErrorCode::kInvalidArgument,
        fmt::format(
            "Buffer of {} bytes cannot hold {}", dst.capacity(), length));
  }
  // The fastest copy that can be reached.
  std::optional<Error> error;
  for (const auto& replica : chunk.replicas) {
    if (!hasBlocks(replica.tier) && !options_.objectStore) {
      continue;
    }
    try {
      co_await co_readReplica(id, replica, offset, length, dst.data());
      dst.setSize(length);
      co_return std::move(dst);
    } catch (const Error& e) {
      if (e.code() != ErrorCode::kUnavailable) {
        throw;
      }
      error = e;
    }
  }
  throw error ? *error
              : Error(
                    ErrorCode::kUnavailable,
                    fmt::format(
                        "No copy of chunk {} can be reached", id.toString()));
}

folly::coro::Task<void> Client::co_readReplica(
    ChunkId id,
    const Replica& replica,
    uint64_t offset,
    uint64_t length,
    uint8_t* dst) {
  if (!hasBlocks(replica.tier)) {
    const auto n = co_await options_.objectStore->co_get(
        chunkObjectKey(id), offset, folly::MutableByteRange(dst, length));
    if (n != length) {
      throw Error(
          ErrorCode::kInternal,
          fmt::format(
              "Object of chunk {} is shorter than the chunk", id.toString()));
    }
    co_return;
  }
  const auto regions = co_await co_regions(replica.blocks);

  // The blocks' parts in [offset, offset + length), read at once.
  std::vector<folly::coro::Task<void>> reads;
  uint64_t blockStart = 0;
  for (size_t i = 0; i < replica.blocks.size(); ++i) {
    const auto& block = replica.blocks[i];
    const auto begin = std::max(offset, blockStart);
    const auto end = std::min(offset + length, blockStart + block.length);
    if (begin < end) {
      reads.push_back(transportFor(regions[i])
                          .co_read(
                              regions[i],
                              block.offset + (begin - blockStart),
                              folly::MutableByteRange(
                                  dst + (begin - offset), end - begin)));
    }
    blockStart += block.length;
  }
  co_await folly::coro::collectAllRange(std::move(reads));
}

folly::coro::Task<ChunkInfo> Client::co_stat(ChunkId id) {
  {
    std::lock_guard lock(mutex_);
    auto it = chunks_.find(id);
    if (it != chunks_.end()) {
      if (std::chrono::steady_clock::now() < it->second.expires) {
        co_return it->second.info;
      }
      chunks_.erase(it);
    }
  }
  auto chunk = co_await metadata_->co_lookup(id);
  cacheChunk(chunk);
  co_return chunk;
}

folly::coro::Task<bool> Client::co_remove(ChunkId id) {
  {
    std::lock_guard lock(mutex_);
    chunks_.erase(id);
  }
  co_return co_await metadata_->co_remove(id);
}

folly::coro::Task<BucketId> Client::co_createBucket() {
  co_return co_await metadata_->co_createBucket();
}

folly::coro::Task<bool> Client::co_deleteBucket(
    BucketId bucket,
    std::chrono::milliseconds grace) {
  {
    std::lock_guard lock(mutex_);
    std::erase_if(chunks_, [&](const auto& entry) {
      return entry.second.info.bucket == bucket;
    });
  }
  co_return co_await metadata_->co_deleteBucket(bucket, grace);
}

folly::coro::Task<std::optional<Ref>> Client::co_getRef(std::string name) {
  co_return co_await metadata_->co_getRef(std::move(name));
}

folly::coro::Task<std::optional<Ref>> Client::co_casRef(
    std::string name,
    std::optional<uint64_t> expectedVersion,
    std::string value) {
  co_return co_await metadata_->co_casRef(
      std::move(name), expectedVersion, std::move(value));
}

folly::coro::Task<std::vector<Ref>> Client::co_listRefs(
    std::string prefix,
    std::string startAfter,
    uint32_t limit) {
  if (limit != 0) {
    co_return co_await metadata_->co_listRefs(
        std::move(prefix), std::move(startAfter), limit);
  }
  std::vector<Ref> refs;
  for (;;) {
    auto page =
        co_await metadata_->co_listRefs(prefix, startAfter, kRefsPerPage);
    for (auto& ref : page) {
      refs.push_back(std::move(ref));
    }
    if (page.size() < kRefsPerPage) {
      co_return refs;
    }
    startAfter = refs.back().name;
  }
}

folly::coro::Task<bool> Client::co_removeRef(
    std::string name,
    std::optional<uint64_t> expectedVersion) {
  co_return co_await metadata_->co_removeRef(
      std::move(name), expectedVersion);
}

folly::coro::Task<std::vector<RegionInfo>> Client::co_regions(
    const std::vector<BlockRef>& blocks) {
  std::vector<RegionId> missing;
  {
    std::lock_guard lock(mutex_);
    for (const auto& block : blocks) {
      if (!regions_.contains(block.region) &&
          std::find(missing.begin(), missing.end(), block.region) ==
              missing.end()) {
        missing.push_back(block.region);
      }
    }
  }
  for (size_t i = 0; i < missing.size(); i += kRegionsPerCall) {
    std::vector<RegionId> ids(
        missing.begin() + i,
        missing.begin() + std::min(i + kRegionsPerCall, missing.size()));
    auto infos = co_await metadata_->co_getRegions(std::move(ids));
    std::lock_guard lock(mutex_);
    for (auto& info : infos) {
      regions_[info.id] = std::move(info);
    }
  }
  std::vector<RegionInfo> result;
  result.reserve(blocks.size());
  std::lock_guard lock(mutex_);
  for (const auto& block : blocks) {
    result.push_back(regions_.at(block.region));
  }
  co_return result;
}

Transport& Client::transportFor(const RegionInfo& region) {
  if (!options_.dataForceTcp && options_.useShm && shm_->canReach(region)) {
    return *shm_;
  }
  if (rdma_ && rdma_->canReach(region)) {
    return *rdma_;
  }
  if (tcp_->canReach(region)) {
    return *tcp_;
  }
  throw Error(
      ErrorCode::kUnavailable,
      fmt::format("Cannot reach region {} of {}", region.id, region.server));
}

void Client::cacheChunk(const ChunkInfo& chunk) {
  std::lock_guard lock(mutex_);
  if (chunks_.size() >= kMaxCachedChunks) {
    chunks_.clear();
  }
  chunks_[chunk.id] = CachedChunk{
      .info = chunk,
      .expires = std::chrono::steady_clock::now() + options_.chunkCacheTtl};
}

ChunkWriter::ChunkWriter(
    Client& client,
    BucketId bucket,
    Placement placement,
    uint64_t sizeHint)
    : client_{client},
      bucket_{bucket},
      placement_{placement},
      sizeHint_{sizeHint},
      hasher_{std::make_unique<ChunkHasher>()} {
  if (bucket != kDefaultBucket) {
    // The id covers the bucket's id, little endian, then the chunk's bytes.
    const auto id = folly::Endian::little(bucket);
    hasher_->update(folly::ByteRange(
        reinterpret_cast<const uint8_t*>(&id), sizeof(id)));
  }
  replicas_.push_back(Replica{.tier = placement.tier});
  if (placement.writeThrough && hasBlocks(*placement.writeThrough)) {
    replicas_.push_back(Replica{.tier = *placement.writeThrough});
  }
  std::sort(
      replicas_.begin(), replicas_.end(), [](const auto& a, const auto& b) {
        return a.tier < b.tier;
      });
}

ChunkWriter::~ChunkWriter() = default;

void ChunkWriter::checkOpen() const {
  if (closed_) {
    throw Error(
        ErrorCode::kInvalidArgument,
        "Chunk writer is committed, aborted or failed");
  }
}

folly::coro::Task<Buffer> ChunkWriter::co_append(Buffer&& data) {
  checkOpen();
  auto bytes = data.bytes();
  if (bytes.empty()) {
    co_return std::move(data);
  }
  // A failed append leaves the chunk incomplete; only co_abort is left.
  closed_ = true;
  std::vector<folly::coro::Task<void>> appends;
  for (auto& replica : replicas_) {
    appends.push_back(co_appendTo(replica, bytes));
  }
  co_await folly::coro::collectAllRange(std::move(appends));
  hasher_->update(bytes);
  size_ += bytes.size();
  closed_ = false;
  co_return std::move(data);
}

folly::coro::Task<void> ChunkWriter::co_appendTo(
    Replica& replica,
    folly::ByteRange bytes) {
  auto& blocks = replica.blocks;
  // Fill the first block with room, then the next ones: the size hint may
  // have allocated some ahead. Then new ones.
  size_t first = blocks.size();
  while (first > 0 && blocks[first - 1].length < kBlockSize) {
    --first;
  }
  uint64_t room = 0;
  for (size_t i = first; i < blocks.size(); ++i) {
    room += kBlockSize - blocks[i].length;
  }
  if (bytes.size() > room) {
    auto count = (bytes.size() - room + kBlockSize - 1) / kBlockSize;
    std::optional<Tier> overflow;
    if (blocks.empty()) {
      count = std::max(count, (sizeHint_ + kBlockSize - 1) / kBlockSize);
      overflow = placement_.overflow;
    }
    auto allocated = co_await client_.metadata_->co_allocate(
        replica.tier, static_cast<uint32_t>(count), overflow);
    for (auto& block : allocated) {
      block.length = 0;
      blocks.push_back(block);
    }
  }
  // The blocks this append writes to; the size hint may have allocated
  // more.
  size_t last = first;
  for (uint64_t covered = 0; covered < bytes.size(); ++last) {
    covered += kBlockSize - blocks[last].length;
  }
  std::vector<BlockRef> touched(blocks.begin() + first, blocks.begin() + last);
  const auto regions = co_await client_.co_regions(touched);
  if (placement_.overflow) {
    // The first allocation chose the tier; later ones stay in it.
    replica.tier = regions[0].tier;
    placement_.tier = replica.tier;
    placement_.overflow.reset();
  }

  std::vector<folly::coro::Task<void>> writes;
  size_t done = 0;
  for (size_t i = first; done < bytes.size(); ++i) {
    auto& block = blocks[i];
    const auto n =
        std::min<uint64_t>(kBlockSize - block.length, bytes.size() - done);
    const auto& region = regions[i - first];
    writes.push_back(client_.transportFor(region).co_write(
        region, block.offset + block.length, bytes.subpiece(done, n)));
    block.length += n;
    done += n;
  }
  co_await folly::coro::collectAllRange(std::move(writes));
}

folly::coro::Task<ChunkId> ChunkWriter::co_commit() {
  checkOpen();
  closed_ = true;
  placement_.overflow.reset();
  // Blocks the size hint allocated beyond the chunk's bytes.
  std::vector<BlockRef> unused;
  for (auto& replica : replicas_) {
    while (!replica.blocks.empty() && replica.blocks.back().length == 0) {
      unused.push_back(replica.blocks.back());
      replica.blocks.pop_back();
    }
  }
  if (!unused.empty()) {
    co_await client_.metadata_->co_free(std::move(unused));
  }
  const auto id = hasher_->id();
  auto replicas = replicas_;
  if (placement_.writeThrough == Tier::kObjectStore) {
    // The object store takes whole objects: read the chunk back.
    auto data = client_.allocateBuffer(std::max<uint64_t>(size_, 1));
    co_await client_.co_readReplica(id, replicas_[0], 0, size_, data.data());
    co_await client_.options_.objectStore->co_put(
        chunkObjectKey(id), folly::ByteRange(data.data(), size_));
    Replica object{.tier = Tier::kObjectStore};
    replicas.push_back(std::move(object));
  }
  // A named argument: GCC 12 destroys temporaries in a co_await's
  // arguments twice.
  ChunkInfo committing{
      .id = id,
      .bucket = bucket_,
      .size = size_,
      .placement = placement_,
      .replicas = std::move(replicas)};
  auto chunk = co_await client_.metadata_->co_commit(std::move(committing));
  client_.cacheChunk(chunk);
  co_return chunk.id;
}

folly::coro::Task<void> ChunkWriter::co_abort() {
  closed_ = true;
  std::vector<BlockRef> blocks;
  for (auto& replica : replicas_) {
    blocks.insert(blocks.end(), replica.blocks.begin(), replica.blocks.end());
    replica.blocks.clear();
  }
  if (!blocks.empty()) {
    co_await client_.metadata_->co_free(std::move(blocks));
  }
}

} // namespace kiln::client
