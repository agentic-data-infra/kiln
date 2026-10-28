#include "kiln/meta/Metadata.h"

#include <algorithm>
#include <set>

#include <fmt/format.h>
#include <folly/Random.h>
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
  // A local-FS region's id, server, tier and length.
  kRegionSet = 4,
  // A region's id.
  kRegionRemoved = 5,
  // A written-through chunk's id, size and placement, and its copy in the
  // write-through tier.
  kChunkSet = 6,
  // A chunk's id: its copy in the write-through tier is gone.
  kChunkRemoved = 7,
};

constexpr uint8_t kNoTier = 0xff;

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

RecordWriter refRecord(const Ref& ref) {
  RecordWriter record;
  record.u8(kRefSet).string(ref.name).string(ref.value).u64(ref.version);
  return record;
}

RecordWriter regionRecord(const RegionInfo& region) {
  RecordWriter record;
  record.u8(kRegionSet)
      .u64(region.id)
      .string(region.server)
      .u8(static_cast<uint8_t>(region.tier))
      .u64(region.length);
  return record;
}

RecordWriter chunkRecord(const ChunkInfo& chunk, const Replica& replica) {
  RecordWriter record;
  record.u8(kChunkSet)
      .u64(chunk.id.high)
      .u64(chunk.id.low)
      .u64(chunk.size)
      .u8(static_cast<uint8_t>(chunk.placement.tier))
      .u8(chunk.placement.writeThrough
              ? static_cast<uint8_t>(*chunk.placement.writeThrough)
              : kNoTier)
      .u8(static_cast<uint8_t>(replica.tier))
      .u64(replica.blocks.size());
  for (const auto& block : replica.blocks) {
    record.u64(block.region).u64(block.offset).u64(block.length);
  }
  return record;
}

Tier toTier(uint8_t tier) {
  if (tier > static_cast<uint8_t>(Tier::kObjectStore)) {
    throw Error(ErrorCode::kInternal, "Bad tier in journal record");
  }
  return static_cast<Tier>(tier);
}

Tier readTier(RecordReader& reader) {
  return toTier(reader.u8());
}

std::optional<Tier> readOptionalTier(RecordReader& reader) {
  const auto tier = reader.u8();
  if (tier == kNoTier) {
    return std::nullopt;
  }
  return toTier(tier);
}

uint64_t blocksOf(const RegionInfo& region) {
  return region.length / kBlockSize;
}

} // namespace

Metadata::Metadata(MetadataOptions options) : options_{std::move(options)} {
  if (options_.journal.empty()) {
    return;
  }
  // Nothing else runs yet, and append() does nothing until journal_ is set.
  auto journal = std::make_unique<Journal>(
      options_.journal, [&](folly::ByteRange record) { replay(record); });
  journal->rewrite(snapshot());
  journal_ = std::move(journal);
}

Metadata::~Metadata() = default;

void Metadata::replay(folly::ByteRange record) {
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
    case kRegionSet: {
      RegionInfo info{.id = reader.u64(), .server = reader.string()};
      info.tier = readTier(reader);
      info.length = reader.u64();
      const auto id = info.id;
      regions_[id] = Region{.info = std::move(info)};
      break;
    }
    case kRegionRemoved:
      regions_.erase(reader.u64());
      break;
    case kChunkSet: {
      ChunkInfo info{.id = {.high = reader.u64(), .low = reader.u64()}};
      info.size = reader.u64();
      info.placement.tier = readTier(reader);
      info.placement.writeThrough = readOptionalTier(reader);
      Replica replica{.tier = readTier(reader)};
      replica.blocks.resize(reader.u64());
      for (auto& block : replica.blocks) {
        block.region = reader.u64();
        block.offset = reader.u64();
        block.length = reader.u64();
      }
      auto it = chunks_.find(info.id);
      if (it != chunks_.end()) {
        eraseChunkLocked(it);
      }
      const auto id = info.id;
      auto& chunk = chunks_.emplace(id, Chunk{.info = std::move(info)})
                        .first->second;
      addReplicaLocked(chunk, std::move(replica));
      break;
    }
    case kChunkRemoved: {
      const ChunkId id{.high = reader.u64(), .low = reader.u64()};
      auto it = chunks_.find(id);
      if (it != chunks_.end()) {
        eraseChunkLocked(it);
      }
      break;
    }
    default:
      throw Error(ErrorCode::kInternal, "Unknown journal record");
  }
}

std::vector<std::string> Metadata::snapshot() {
  std::vector<std::string> records;
  records.push_back(RecordWriter().u8(kNextVersion).u64(nextVersion_).record());
  for (const auto& [name, ref] : refs_) {
    records.push_back(refRecord(ref).record());
  }
  for (const auto& [id, region] : regions_) {
    if (region.info.tier == Tier::kLocalFs) {
      records.push_back(regionRecord(region.info).record());
    }
  }
  for (const auto& [id, chunk] : chunks_) {
    for (const auto& replica : chunk.info.replicas) {
      if (chunk.info.placement.writeThrough == replica.tier) {
        records.push_back(chunkRecord(chunk.info, replica).record());
      }
    }
  }
  return records;
}

void Metadata::append(const RecordWriter& record) {
  std::lock_guard lock(journalMutex_);
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
  std::set<RegionId> kept;
  for (const auto& region : regions) {
    if (region.length < kBlockSize) {
      throw Error(
          ErrorCode::kInvalidArgument,
          fmt::format(
              "Region of {} bytes is smaller than a block", region.length));
    }
    if (region.server != regions[0].server || region.tier != regions[0].tier ||
        !hasBlocks(region.tier)) {
      throw Error(
          ErrorCode::kInvalidArgument,
          "A server registers regions of one server and one tier of blocks");
    }
    if (region.id != 0 && !kept.insert(region.id).second) {
      throw Error(
          ErrorCode::kInvalidArgument,
          fmt::format("Region {} is registered twice", region.id));
    }
    blocks += blocksOf(region);
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
  // This server replaces itself as registered before, and the owners of the
  // regions it reattaches (e.g. it restarted on another port).
  const auto& name = regions[0].server;
  std::set<std::string> replaced{name};
  // Reattached regions that were in use until now keep their free lists.
  std::set<RegionId> live;
  for (auto id : kept) {
    auto it = regions_.find(id);
    if (it != regions_.end()) {
      replaced.insert(it->second.info.server);
      if (servers_.contains(it->second.info.server)) {
        live.insert(id);
      }
    }
  }
  bool dropped = false;
  for (auto it = regions_.begin(); it != regions_.end();) {
    if (replaced.contains(it->second.info.server) &&
        !kept.contains(it->first)) {
      if (it->second.info.tier == Tier::kLocalFs) {
        append(RecordWriter().u8(kRegionRemoved).u64(it->first));
      }
      it = regions_.erase(it);
      dropped = true;
    } else {
      ++it;
    }
  }
  if (dropped) {
    // The copies in the dropped regions are gone.
    for (auto it = chunks_.begin(); it != chunks_.end();) {
      std::optional<Tier> lost;
      for (const auto& replica : it->second.info.replicas) {
        for (const auto& block : replica.blocks) {
          if (!regions_.contains(block.region)) {
            lost = replica.tier;
          }
        }
      }
      it = lost ? dropReplicaLocked(it, *lost) : std::next(it);
    }
  }
  for (const auto& server : replaced) {
    servers_.erase(server);
  }

  for (auto& region : regions) {
    Region state{.info = region};
    if (region.id == 0) {
      do {
        region.id = folly::Random::rand64();
      } while (region.id == 0 || regions_.contains(region.id));
      state.info.id = region.id;
    }
    if (live.contains(region.id)) {
      state.free = std::move(regions_.at(region.id).free);
    } else {
      // Free unless a chunk's copy (known from the journal) is in it.
      std::vector<bool> used(blocksOf(region));
      for (const auto& [id, chunk] : chunks_) {
        for (const auto& replica : chunk.info.replicas) {
          for (const auto& block : replica.blocks) {
            if (block.region == region.id &&
                block.offset / kBlockSize < used.size()) {
              used[block.offset / kBlockSize] = true;
            }
          }
        }
      }
      for (uint64_t i = used.size(); i-- > 0;) {
        if (!used[i]) {
          state.free.push_back(i * kBlockSize);
        }
      }
    }
    if (region.tier == Tier::kLocalFs) {
      append(regionRecord(region));
    }
    regions_[region.id] = std::move(state);
  }

  Server server{.tier = regions[0].tier, .budgetBlocks = blocks};
  std::set<RegionId> ids;
  uint64_t inUse = 0;
  for (const auto& region : regions) {
    ids.insert(region.id);
    inUse += blocksOf(region) - regions_.at(region.id).free.size();
  }
  for (const auto& entry : grace_) {
    if (ids.contains(entry.block.region)) {
      ++server.graceBlocks;
    }
  }
  server.liveBlocks = inUse - server.graceBlocks;
  servers_[name] = server;
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
    Tier tier,
    uint32_t count,
    std::optional<Tier> overflow) {
  for (auto t : {std::optional<Tier>(tier), overflow}) {
    if (t && !hasBlocks(*t)) {
      throw Error(
          ErrorCode::kInvalidArgument,
          fmt::format("The {} tier has no blocks", toString(*t)));
    }
  }
  if (overflow && *overflow != tier) {
    // Checked and taken under one lock, so no other allocation can take
    // the free blocks in between.
    std::lock_guard lock(mutex_);
    reclaimLocked(Clock::now());
    if (freeBlocksLocked(tier) >= count) {
      Clock::duration wait;
      co_return std::move(
          *tryAllocateLocked(tier, count, wait, /*evict=*/false));
    }
    tier = *overflow;
  }
  // Blocks freed before this call are reusable by the end of their grace
  // period; waiting longer would not help.
  const auto deadline =
      Clock::now() + options_.gracePeriod + std::chrono::seconds(1);
  for (;;) {
    Clock::duration wait;
    {
      std::lock_guard lock(mutex_);
      if (auto blocks = tryAllocateLocked(tier, count, wait)) {
        co_return std::move(*blocks);
      }
    }
    if (Clock::now() >= deadline) {
      throw Error(
          ErrorCode::kOutOfSpace,
          fmt::format(
              "No {} blocks in {} became free in time",
              count,
              toString(tier)));
    }
    co_await folly::coro::sleep(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::clamp<Clock::duration>(
                wait, std::chrono::milliseconds(1), options_.gracePeriod)));
  }
}

std::optional<std::vector<BlockRef>> Metadata::tryAllocateLocked(
    Tier tier,
    uint32_t count,
    Clock::duration& wait,
    bool evict) {
  const auto now = Clock::now();
  reclaimLocked(now);

  auto tierBlocks = [&](auto blocksOf) {
    uint64_t sum = 0;
    for (const auto& [name, server] : servers_) {
      if (server.tier == tier) {
        sum += blocksOf(server);
      }
    }
    return sum;
  };
  // Free now or after the grace period. A server may be over its budget if
  // it registered again with a smaller one.
  auto available = [&] {
    return tierBlocks([](const Server& s) {
      return s.budgetBlocks - std::min(s.budgetBlocks, s.liveBlocks);
    });
  };
  const auto budget = tierBlocks([](const Server& s) { return s.budgetBlocks; });

  // Keep some of the tier free, so that allocations rarely wait for the
  // grace period of the blocks they need.
  if (evict && available() < count + options_.evictBelow * budget) {
    evictLocked(
        tier, std::min<uint64_t>(budget, count + options_.evictUntil * budget));
  }
  if (available() < count) {
    throw Error(
        ErrorCode::kOutOfSpace,
        fmt::format(
            "Asked for {} blocks in {}: {} of {} are in chunks being written",
            count,
            toString(tier),
            budget - available(),
            budget));
  }
  const auto freeNow = tierBlocks([](const Server& s) {
    return s.budgetBlocks -
        std::min(s.budgetBlocks, s.liveBlocks + s.graceBlocks);
  });
  if (freeNow < count) {
    // Wait until the oldest freed block of the tier is reusable.
    wait = options_.gracePeriod;
    for (const auto& entry : grace_) {
      auto* server = serverOf(entry.block.region);
      if (server != nullptr && server->tier == tier) {
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
    if (region.info.tier == tier && server != nullptr) {
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
  auto free = [&] {
    uint64_t sum = 0;
    for (const auto& [name, server] : servers_) {
      if (server.tier == tier) {
        sum += server.budgetBlocks -
            std::min(server.budgetBlocks, server.liveBlocks);
      }
    }
    return sum;
  };
  for (auto it = lru.begin(); it != lru.end() && free() < available;) {
    auto chunk = chunks_.find(*it);
    ++it;
    const auto& replicas = chunk->second.info.replicas;
    auto replica = std::find_if(
        replicas.begin(), replicas.end(), [&](const Replica& r) {
          return r.tier == tier;
        });
    // Dropping copies on servers that have not registered (again) frees
    // nothing here.
    if (readableLocked(*replica)) {
      dropReplicaLocked(chunk, tier);
      ++evictions_;
    }
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

bool Metadata::readableLocked(const Replica& replica) const {
  for (const auto& block : replica.blocks) {
    auto region = regions_.find(block.region);
    if (region == regions_.end() ||
        !servers_.contains(region->second.info.server)) {
      return false;
    }
  }
  return true;
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

void Metadata::addReplicaLocked(Chunk& chunk, Replica replica) {
  if (hasBlocks(replica.tier)) {
    auto& lru = lruOf(replica.tier);
    lru.push_back(chunk.info.id);
    chunk.lru[static_cast<size_t>(replica.tier)] = std::prev(lru.end());
  }
  if (chunk.info.placement.writeThrough == replica.tier) {
    append(chunkRecord(chunk.info, replica));
  }
  auto& replicas = chunk.info.replicas;
  replicas.push_back(std::move(replica));
  std::sort(replicas.begin(), replicas.end(), [](const auto& a, const auto& b) {
    return a.tier < b.tier;
  });
}

Metadata::ChunkMap::iterator Metadata::dropReplicaLocked(
    ChunkMap::iterator chunk,
    Tier tier) {
  auto& info = chunk->second.info;
  auto replica = std::find_if(
      info.replicas.begin(), info.replicas.end(), [&](const Replica& r) {
        return r.tier == tier;
      });
  if (replica == info.replicas.end()) {
    return std::next(chunk);
  }
  if (hasBlocks(tier)) {
    lruOf(tier).erase(chunk->second.lru[static_cast<size_t>(tier)]);
    freeLocked(replica->blocks, /*grace=*/true);
  }
  if (info.placement.writeThrough == tier) {
    append(RecordWriter().u8(kChunkRemoved).u64(info.id.high).u64(info.id.low));
  }
  info.replicas.erase(replica);
  if (info.replicas.empty()) {
    return chunks_.erase(chunk);
  }
  return std::next(chunk);
}

void Metadata::eraseChunkLocked(ChunkMap::iterator chunk) {
  std::vector<Tier> tiers;
  for (const auto& replica : chunk->second.info.replicas) {
    tiers.push_back(replica.tier);
  }
  // The last drop erases the chunk.
  for (auto tier : tiers) {
    dropReplicaLocked(chunk, tier);
  }
}

folly::coro::Task<ChunkInfo> Metadata::co_commit(ChunkInfo chunk) {
  std::lock_guard lock(mutex_);
  const auto& placement = chunk.placement;
  if (!hasBlocks(placement.tier) || placement.writeThrough == placement.tier) {
    throw Error(
        ErrorCode::kInvalidArgument,
        fmt::format(
            "Bad placement in {} writing through to {}",
            toString(placement.tier),
            placement.writeThrough ? toString(*placement.writeThrough)
                                   : "nothing"));
  }
  std::vector<Tier> tiers{placement.tier};
  if (placement.writeThrough) {
    tiers.push_back(*placement.writeThrough);
  }
  std::sort(tiers.begin(), tiers.end());
  if (chunk.replicas.size() != tiers.size()) {
    throw Error(
        ErrorCode::kInvalidArgument,
        fmt::format(
            "Chunk with {} copies for {} tiers",
            chunk.replicas.size(),
            tiers.size()));
  }
  for (size_t i = 0; i < tiers.size(); ++i) {
    const auto& replica = chunk.replicas[i];
    if (replica.tier != tiers[i]) {
      throw Error(
          ErrorCode::kInvalidArgument,
          fmt::format("Copy in {} out of place", toString(replica.tier)));
    }
    if (!hasBlocks(replica.tier)) {
      if (!replica.blocks.empty()) {
        throw Error(
            ErrorCode::kInvalidArgument,
            fmt::format("Copy in {} with blocks", toString(replica.tier)));
      }
      continue;
    }
    uint64_t size = 0;
    for (const auto& block : replica.blocks) {
      auto it = regions_.find(block.region);
      if (it == regions_.end() || block.offset % kBlockSize != 0 ||
          block.length > kBlockSize ||
          block.offset + kBlockSize > it->second.info.length ||
          it->second.info.tier != replica.tier) {
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
              "Chunk of {} bytes has {} bytes of blocks in {}",
              chunk.size,
              size,
              toString(replica.tier)));
    }
  }

  auto it = chunks_.find(chunk.id);
  if (it == chunks_.end()) {
    const auto id = chunk.id;
    auto replicas = std::move(chunk.replicas);
    chunk.replicas.clear();
    it = chunks_.emplace(id, Chunk{.info = std::move(chunk)}).first;
    for (auto& replica : replicas) {
      addReplicaLocked(it->second, std::move(replica));
    }
    co_return it->second.info;
  }
  // The first commit wins; the same bytes are stored already. Copies in
  // tiers it has none in are added, the others dropped: nobody has seen
  // their blocks.
  auto& existing = it->second;
  for (auto& replica : chunk.replicas) {
    const bool present = std::any_of(
        existing.info.replicas.begin(),
        existing.info.replicas.end(),
        [&](const Replica& r) { return r.tier == replica.tier; });
    if (present) {
      freeLocked(replica.blocks, /*grace=*/false);
      continue;
    }
    if (!existing.info.placement.writeThrough &&
        placement.writeThrough == replica.tier) {
      existing.info.placement.writeThrough = replica.tier;
    }
    addReplicaLocked(existing, std::move(replica));
  }
  ChunkInfo result = existing.info;
  std::erase_if(result.replicas, [&](const Replica& r) {
    return !readableLocked(r);
  });
  co_return result;
}

folly::coro::Task<ChunkInfo> Metadata::co_lookup(ChunkId id) {
  std::lock_guard lock(mutex_);
  auto it = chunks_.find(id);
  if (it == chunks_.end()) {
    throw Error(
        ErrorCode::kNotFound, fmt::format("No chunk {}", id.toString()));
  }
  ChunkInfo result = it->second.info;
  std::erase_if(result.replicas, [&](const Replica& r) {
    return !readableLocked(r);
  });
  if (result.replicas.empty()) {
    throw Error(
        ErrorCode::kUnavailable,
        fmt::format(
            "The storage servers of chunk {} have not registered",
            id.toString()));
  }
  for (const auto& replica : result.replicas) {
    if (hasBlocks(replica.tier)) {
      auto& lru = lruOf(replica.tier);
      lru.splice(
          lru.end(), lru, it->second.lru[static_cast<size_t>(replica.tier)]);
    }
  }
  co_return result;
}

folly::coro::Task<bool> Metadata::co_remove(ChunkId id) {
  std::lock_guard lock(mutex_);
  auto it = chunks_.find(id);
  if (it == chunks_.end()) {
    co_return false;
  }
  eraseChunkLocked(it);
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
  append(RecordWriter()
                   .u8(kRefSet)
                   .string(ref.name)
                   .string(ref.value)
                   .u64(ref.version));
  ++nextVersion_;
  refs_[std::move(name)] = ref;
  co_return ref;
}

folly::coro::Task<std::vector<Ref>> Metadata::co_listRefs(
    std::string prefix,
    std::string startAfter,
    uint32_t limit) {
  std::lock_guard lock(refMutex_);
  std::vector<Ref> result;
  auto it = startAfter.empty() ? refs_.lower_bound(prefix)
                               : refs_.upper_bound(std::max(startAfter, prefix));
  for (; it != refs_.end() && it->first.starts_with(prefix) &&
       (limit == 0 || result.size() < limit);
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
  append(RecordWriter().u8(kRefRemoved).string(name));
  refs_.erase(it);
  co_return true;
}

size_t Metadata::freeBlocks(Tier tier) const {
  std::lock_guard lock(mutex_);
  return freeBlocksLocked(tier);
}

size_t Metadata::freeBlocksLocked(Tier tier) const {
  size_t count = 0;
  for (const auto& [name, server] : servers_) {
    if (server.tier == tier) {
      count += server.budgetBlocks -
          std::min(server.budgetBlocks, server.liveBlocks);
    }
  }
  return count - std::min(count, graceBlocksLocked(tier));
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
