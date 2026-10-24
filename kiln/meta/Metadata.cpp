#include "kiln/meta/Metadata.h"

#include <algorithm>

#include <fmt/format.h>
#include <folly/coro/Sleep.h>

#include "kiln/common/Error.h"

namespace kiln::meta {

namespace {

// Journal records, by their first byte.
enum RecordKind : uint8_t {
  // A ref's name, value and version.
  kRefSet = 1,
  // A ref's name.
  kRefRemoved = 2,
  // The version of the next ref update, so that versions of removed refs
  // are not reused after the journal is rewritten.
  kNextVersion = 3,
};

void checkRefName(const std::string& name) {
  if (name.empty() || name.size() > kMaxRefNameSize) {
    throw Error(
        ErrorCode::kInvalidArgument,
        fmt::format(
            "Ref name of {} bytes, not 1 to {}",
            name.size(),
            kMaxRefNameSize));
  }
}

} // namespace

Metadata::Metadata(MetadataOptions options) : options_{std::move(options)} {
  if (options_.journal.empty()) {
    return;
  }
  std::lock_guard lock(refMutex_);
  journal_ = std::make_unique<Journal>(
      options_.journal, [&](folly::ByteRange record) { replayLocked(record); });
  // Keep only the records that make up the current state.
  std::vector<std::string> records;
  records.push_back(RecordWriter().u8(kNextVersion).u64(nextVersion_).record());
  for (const auto& [name, ref] : refs_) {
    records.push_back(RecordWriter()
                          .u8(kRefSet)
                          .string(ref.name)
                          .string(ref.value)
                          .u64(ref.version)
                          .record());
  }
  journal_->rewrite(records);
}

Metadata::~Metadata() = default;

void Metadata::replayLocked(folly::ByteRange record) {
  RecordReader reader(record);
  switch (reader.u8()) {
    case kRefSet: {
      Ref ref{.name = reader.string(), .value = reader.string()};
      ref.version = reader.u64();
      nextVersion_ = std::max(nextVersion_, ref.version + 1);
      auto name = ref.name;
      refs_[std::move(name)] = std::move(ref);
      break;
    }
    case kRefRemoved:
      refs_.erase(reader.string());
      break;
    case kNextVersion:
      nextVersion_ = std::max(nextVersion_, reader.u64());
      break;
    default:
      throw Error(ErrorCode::kInternal, "Unknown journal record");
  }
}

void Metadata::appendLocked(const RecordWriter& record) {
  if (journal_) {
    journal_->append(folly::StringPiece(record.record()));
  }
}

folly::coro::Task<std::vector<RegionInfo>> Metadata::co_registerServer(
    std::vector<RegionInfo> regions,
    uint64_t budget) {
  if (regions.empty()) {
    throw Error(ErrorCode::kInvalidArgument, "No regions to register");
  }
  uint64_t blocks = 0;
  for (const auto& region : regions) {
    if (region.length < kBlockSize) {
      throw Error(
          ErrorCode::kInvalidArgument,
          fmt::format(
              "Region of {} bytes is smaller than a block", region.length));
    }
    if (region.server != regions[0].server || region.tier != regions[0].tier) {
      throw Error(
          ErrorCode::kInvalidArgument,
          "A server registers regions of one server and one tier");
    }
    blocks += region.length / kBlockSize;
  }
  if (budget != 0) {
    if (budget < kBlockSize) {
      throw Error(
          ErrorCode::kInvalidArgument,
          fmt::format("Budget of {} bytes is less than a block", budget));
    }
    blocks = std::min(blocks, budget / kBlockSize);
  }

  std::lock_guard lock(mutex_);
  // Forget the server's old regions and the chunks in them.
  const auto& name = regions[0].server;
  std::erase_if(regions_, [&](const auto& entry) {
    return entry.second.info.server == name;
  });
  servers_.erase(name);
  for (auto it = chunks_.begin(); it != chunks_.end();) {
    const auto& chunkBlocks = it->second.info.blocks;
    const bool lost = std::any_of(
        chunkBlocks.begin(), chunkBlocks.end(), [&](const BlockRef& block) {
          return !regions_.contains(block.region);
        });
    auto next = std::next(it);
    if (lost) {
      eraseChunkLocked(it, /*grace=*/true);
    }
    it = next;
  }

  servers_.emplace(
      name, Server{.tier = regions[0].tier, .budgetBlocks = blocks});
  for (auto& region : regions) {
    region.id = nextRegionId_++;
    Region state{.info = region};
    const auto regionBlocks = region.length / kBlockSize;
    state.free.reserve(regionBlocks);
    for (uint64_t i = regionBlocks; i-- > 0;) {
      state.free.push_back(i * kBlockSize);
    }
    regions_.emplace(region.id, std::move(state));
  }
  co_return regions;
}

folly::coro::Task<std::vector<RegionInfo>> Metadata::co_getRegions(
    std::vector<RegionId> ids) {
  std::lock_guard lock(mutex_);
  std::vector<RegionInfo> result;
  result.reserve(ids.size());
  for (auto id : ids) {
    auto it = regions_.find(id);
    if (it == regions_.end()) {
      throw Error(ErrorCode::kNotFound, fmt::format("No region {}", id));
    }
    result.push_back(it->second.info);
  }
  co_return result;
}

folly::coro::Task<std::vector<BlockRef>> Metadata::co_allocate(
    Placement placement,
    uint32_t count) {
  // Blocks freed before this call are reusable by the end of their grace
  // period; waiting longer would not help.
  const auto deadline =
      Clock::now() + options_.gracePeriod + std::chrono::seconds(1);
  for (;;) {
    Clock::duration wait;
    {
      std::lock_guard lock(mutex_);
      if (auto blocks = tryAllocateLocked(placement, count, wait)) {
        co_return std::move(*blocks);
      }
    }
    if (Clock::now() >= deadline) {
      throw Error(
          ErrorCode::kOutOfSpace,
          fmt::format(
              "No {} blocks in {} became free in time",
              count,
              toString(placement.tier)));
    }
    co_await folly::coro::sleep(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::clamp<Clock::duration>(
                wait, std::chrono::milliseconds(1), options_.gracePeriod)));
  }
}

std::optional<std::vector<BlockRef>> Metadata::tryAllocateLocked(
    Placement placement,
    uint32_t count,
    Clock::duration& wait) {
  const auto now = Clock::now();
  reclaimLocked(now);

  auto tierBlocks = [&](auto blocksOf) {
    uint64_t sum = 0;
    for (const auto& [name, server] : servers_) {
      if (server.tier == placement.tier) {
        sum += blocksOf(server);
      }
    }
    return sum;
  };
  const auto budget = tierBlocks([](const Server& s) { return s.budgetBlocks; });
  auto available = [&] {
    // Free now or after the grace period.
    return budget - tierBlocks([](const Server& s) { return s.liveBlocks; });
  };

  // Keep some of the tier free, so that allocations rarely wait for the
  // grace period of the blocks they need.
  if (available() < count + options_.evictBelow * budget) {
    evictLocked(
        placement.tier,
        std::min<uint64_t>(budget, count + options_.evictUntil * budget));
  }
  if (available() < count) {
    throw Error(
        ErrorCode::kOutOfSpace,
        fmt::format(
            "Asked for {} blocks in {}: {} of {} are in chunks being written",
            count,
            toString(placement.tier),
            budget - available(),
            budget));
  }
  const auto freeNow = tierBlocks([](const Server& s) {
    return s.budgetBlocks - s.liveBlocks - s.graceBlocks;
  });
  if (freeNow < count) {
    // Wait until the oldest freed block of the tier is reusable.
    wait = options_.gracePeriod;
    for (const auto& entry : grace_) {
      auto* server = serverOf(entry.block.region);
      if (server != nullptr && server->tier == placement.tier) {
        wait = entry.reusableAt - now;
        break;
      }
    }
    return std::nullopt;
  }

  // Round robin over the tier's regions, starting after where the last
  // allocation ended.
  std::vector<std::pair<Region*, Server*>> candidates;
  auto consider = [&](Region& region) {
    auto* server = serverOf(region.info.id);
    if (region.info.tier == placement.tier && server != nullptr) {
      candidates.emplace_back(&region, server);
    }
  };
  for (auto it = regions_.upper_bound(nextAllocation_); it != regions_.end();
       ++it) {
    consider(it->second);
  }
  for (auto it = regions_.begin();
       it != regions_.end() && it->first <= nextAllocation_;
       ++it) {
    consider(it->second);
  }

  std::vector<BlockRef> blocks;
  blocks.reserve(count);
  for (size_t i = 0; blocks.size() < count; ++i) {
    auto [region, server] = candidates[i % candidates.size()];
    if (region->free.empty() ||
        server->liveBlocks + server->graceBlocks >= server->budgetBlocks) {
      continue;
    }
    blocks.push_back(BlockRef{
        .region = region->info.id,
        .offset = region->free.back(),
        .length = kBlockSize});
    region->free.pop_back();
    ++server->liveBlocks;
    nextAllocation_ = region->info.id;
  }
  return blocks;
}

void Metadata::evictLocked(Tier tier, uint64_t available) {
  auto& lru = lruOf(tier);
  auto live = [&] {
    uint64_t sum = 0;
    for (const auto& [name, server] : servers_) {
      if (server.tier == tier) {
        sum += server.liveBlocks;
      }
    }
    return sum;
  };
  uint64_t budget = 0;
  for (const auto& [name, server] : servers_) {
    if (server.tier == tier) {
      budget += server.budgetBlocks;
    }
  }
  while (!lru.empty() && budget - live() < available) {
    eraseChunkLocked(chunks_.find(lru.front()), /*grace=*/true);
    ++evictions_;
  }
}

void Metadata::reclaimLocked(Clock::time_point now) {
  while (!grace_.empty() && grace_.front().reusableAt <= now) {
    const auto& block = grace_.front().block;
    // Blocks of regions that are gone stay gone.
    auto region = regions_.find(block.region);
    auto* server = serverOf(block.region);
    if (region != regions_.end() && server != nullptr) {
      region->second.free.push_back(block.offset);
      --server->graceBlocks;
    }
    grace_.pop_front();
  }
}

Metadata::Server* Metadata::serverOf(RegionId region) {
  auto it = regions_.find(region);
  if (it == regions_.end()) {
    return nullptr;
  }
  auto server = servers_.find(it->second.info.server);
  return server == servers_.end() ? nullptr : &server->second;
}

folly::coro::Task<void> Metadata::co_free(std::vector<BlockRef> blocks) {
  std::lock_guard lock(mutex_);
  freeLocked(blocks, /*grace=*/false);
  co_return;
}

void Metadata::freeLocked(const std::vector<BlockRef>& blocks, bool grace) {
  const auto reusableAt = Clock::now() + options_.gracePeriod;
  for (const auto& block : blocks) {
    auto* server = serverOf(block.region);
    if (server == nullptr) {
      continue;
    }
    --server->liveBlocks;
    if (grace) {
      ++server->graceBlocks;
      grace_.push_back(GraceBlock{.block = block, .reusableAt = reusableAt});
    } else {
      regions_.at(block.region).free.push_back(block.offset);
    }
  }
}

void Metadata::eraseChunkLocked(
    std::unordered_map<ChunkId, Chunk>::iterator chunk,
    bool grace) {
  lruOf(chunk->second.info.placement.tier).erase(chunk->second.lru);
  freeLocked(chunk->second.info.blocks, grace);
  chunks_.erase(chunk);
}

folly::coro::Task<ChunkInfo> Metadata::co_commit(ChunkInfo chunk) {
  std::lock_guard lock(mutex_);
  uint64_t size = 0;
  for (const auto& block : chunk.blocks) {
    auto it = regions_.find(block.region);
    if (it == regions_.end() || block.offset % kBlockSize != 0 ||
        block.length > kBlockSize ||
        block.offset + kBlockSize > it->second.info.length ||
        it->second.info.tier != chunk.placement.tier) {
      throw Error(
          ErrorCode::kInvalidArgument,
          fmt::format(
              "Bad block region {} offset {} length {}",
              block.region,
              block.offset,
              block.length));
    }
    size += block.length;
  }
  if (size != chunk.size) {
    throw Error(
        ErrorCode::kInvalidArgument,
        fmt::format(
            "Chunk of {} bytes has {} bytes of blocks", chunk.size, size));
  }

  auto it = chunks_.find(chunk.id);
  if (it != chunks_.end()) {
    // The first commit wins; the same bytes are stored already. Nobody has
    // seen this copy's blocks.
    freeLocked(chunk.blocks, /*grace=*/false);
    co_return it->second.info;
  }
  auto& lru = lruOf(chunk.placement.tier);
  lru.push_back(chunk.id);
  const auto id = chunk.id;
  it = chunks_
           .emplace(id, Chunk{.info = std::move(chunk), .lru = std::prev(lru.end())})
           .first;
  co_return it->second.info;
}

folly::coro::Task<ChunkInfo> Metadata::co_lookup(ChunkId id) {
  std::lock_guard lock(mutex_);
  auto it = chunks_.find(id);
  if (it == chunks_.end()) {
    throw Error(
        ErrorCode::kNotFound, fmt::format("No chunk {}", id.toString()));
  }
  auto& lru = lruOf(it->second.info.placement.tier);
  lru.splice(lru.end(), lru, it->second.lru);
  co_return it->second.info;
}

folly::coro::Task<bool> Metadata::co_remove(ChunkId id) {
  std::lock_guard lock(mutex_);
  auto it = chunks_.find(id);
  if (it == chunks_.end()) {
    co_return false;
  }
  eraseChunkLocked(it, /*grace=*/true);
  co_return true;
}

folly::coro::Task<std::optional<Ref>> Metadata::co_getRef(std::string name) {
  std::lock_guard lock(refMutex_);
  auto it = refs_.find(name);
  if (it == refs_.end()) {
    co_return std::nullopt;
  }
  co_return it->second;
}

folly::coro::Task<std::optional<Ref>> Metadata::co_casRef(
    std::string name,
    std::optional<uint64_t> expectedVersion,
    std::string value) {
  checkRefName(name);
  if (value.size() > kMaxRefValueSize) {
    throw Error(
        ErrorCode::kInvalidArgument,
        fmt::format(
            "Ref value of {} bytes is over {}",
            value.size(),
            kMaxRefValueSize));
  }
  std::lock_guard lock(refMutex_);
  auto it = refs_.find(name);
  const auto version = it == refs_.end()
      ? std::optional<uint64_t>()
      : std::optional<uint64_t>(it->second.version);
  if (version != expectedVersion) {
    co_return std::nullopt;
  }
  Ref ref{.name = name, .value = std::move(value), .version = nextVersion_};
  appendLocked(RecordWriter()
                   .u8(kRefSet)
                   .string(ref.name)
                   .string(ref.value)
                   .u64(ref.version));
  ++nextVersion_;
  refs_[std::move(name)] = ref;
  co_return ref;
}

folly::coro::Task<std::vector<Ref>> Metadata::co_listRefs(std::string prefix) {
  std::lock_guard lock(refMutex_);
  std::vector<Ref> result;
  for (auto it = refs_.lower_bound(prefix);
       it != refs_.end() && it->first.starts_with(prefix);
       ++it) {
    result.push_back(it->second);
  }
  co_return result;
}

folly::coro::Task<bool> Metadata::co_removeRef(
    std::string name,
    std::optional<uint64_t> expectedVersion) {
  std::lock_guard lock(refMutex_);
  auto it = refs_.find(name);
  if (it == refs_.end() ||
      (expectedVersion && it->second.version != *expectedVersion)) {
    co_return false;
  }
  appendLocked(RecordWriter().u8(kRefRemoved).string(name));
  refs_.erase(it);
  co_return true;
}

size_t Metadata::freeBlocks(Tier tier) const {
  std::lock_guard lock(mutex_);
  size_t count = 0;
  for (const auto& [name, server] : servers_) {
    if (server.tier == tier) {
      count += server.budgetBlocks - server.liveBlocks;
    }
  }
  return count - graceBlocksLocked(tier);
}

size_t Metadata::graceBlocks(Tier tier) const {
  std::lock_guard lock(mutex_);
  return graceBlocksLocked(tier);
}

size_t Metadata::graceBlocksLocked(Tier tier) const {
  const auto now = Clock::now();
  size_t count = 0;
  for (const auto& entry : grace_) {
    auto region = regions_.find(entry.block.region);
    if (entry.reusableAt > now && region != regions_.end() &&
        region->second.info.tier == tier &&
        servers_.contains(region->second.info.server)) {
      ++count;
    }
  }
  return count;
}

size_t Metadata::chunkCount() const {
  std::lock_guard lock(mutex_);
  return chunks_.size();
}

size_t Metadata::evictions() const {
  std::lock_guard lock(mutex_);
  return evictions_;
}

} // namespace kiln::meta
