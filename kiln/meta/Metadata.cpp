#include "kiln/meta/Metadata.h"

#include <algorithm>

#include <fmt/format.h>

#include "kiln/common/Error.h"

namespace kiln::meta {

folly::coro::Task<std::vector<RegionInfo>> Metadata::co_registerServer(
    std::vector<RegionInfo> regions) {
  std::lock_guard lock(mutex_);
  for (const auto& region : regions) {
    if (region.length < kBlockSize) {
      throw Error(
          ErrorCode::kInvalidArgument,
          fmt::format(
              "Region of {} bytes is smaller than a block", region.length));
    }
  }

  // Forget the server's old regions and the chunks in them.
  for (const auto& region : regions) {
    std::erase_if(regions_, [&](const auto& entry) {
      return entry.second.info.server == region.server;
    });
  }
  std::erase_if(chunks_, [&](const auto& entry) {
    return std::any_of(
        entry.second.blocks.begin(),
        entry.second.blocks.end(),
        [&](const BlockRef& block) { return !regions_.contains(block.region); });
  });

  for (auto& region : regions) {
    region.id = nextRegionId_++;
    Region state{.info = region};
    const auto blocks = region.length / kBlockSize;
    state.free.reserve(blocks);
    for (uint64_t i = blocks; i-- > 0;) {
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
  std::lock_guard lock(mutex_);
  std::vector<Region*> candidates;
  size_t available = 0;
  // Start the round after where the last one ended.
  for (auto it = regions_.upper_bound(nextAllocation_); it != regions_.end();
       ++it) {
    candidates.push_back(&it->second);
  }
  for (auto it = regions_.begin();
       it != regions_.end() && it->first <= nextAllocation_;
       ++it) {
    candidates.push_back(&it->second);
  }
  std::erase_if(candidates, [&](Region* region) {
    return region->info.tier != placement.tier || region->free.empty();
  });
  for (auto* region : candidates) {
    available += region->free.size();
  }
  if (available < count) {
    throw Error(
        ErrorCode::kOutOfSpace,
        fmt::format(
            "Asked for {} blocks in {}, {} free",
            count,
            toString(placement.tier),
            available));
  }

  std::vector<BlockRef> blocks;
  blocks.reserve(count);
  for (size_t i = 0; blocks.size() < count; ++i) {
    auto* region = candidates[i % candidates.size()];
    if (region->free.empty()) {
      continue;
    }
    blocks.push_back(BlockRef{
        .region = region->info.id,
        .offset = region->free.back(),
        .length = kBlockSize});
    region->free.pop_back();
    nextAllocation_ = region->info.id;
  }
  co_return blocks;
}

folly::coro::Task<void> Metadata::co_free(std::vector<BlockRef> blocks) {
  std::lock_guard lock(mutex_);
  freeLocked(blocks);
  co_return;
}

void Metadata::freeLocked(const std::vector<BlockRef>& blocks) {
  for (const auto& block : blocks) {
    // Blocks of regions that are gone stay gone.
    auto it = regions_.find(block.region);
    if (it != regions_.end()) {
      it->second.free.push_back(block.offset);
    }
  }
}

folly::coro::Task<ChunkInfo> Metadata::co_commit(ChunkInfo chunk) {
  std::lock_guard lock(mutex_);
  uint64_t size = 0;
  for (const auto& block : chunk.blocks) {
    auto it = regions_.find(block.region);
    if (it == regions_.end() || block.offset % kBlockSize != 0 ||
        block.length > kBlockSize ||
        block.offset + kBlockSize > it->second.info.length) {
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

  auto [it, inserted] = chunks_.try_emplace(chunk.id, chunk);
  if (!inserted) {
    // The first commit wins; the same bytes are stored already.
    freeLocked(chunk.blocks);
  }
  co_return it->second;
}

folly::coro::Task<ChunkInfo> Metadata::co_lookup(ChunkId id) {
  std::lock_guard lock(mutex_);
  auto it = chunks_.find(id);
  if (it == chunks_.end()) {
    throw Error(
        ErrorCode::kNotFound, fmt::format("No chunk {}", id.toString()));
  }
  co_return it->second;
}

folly::coro::Task<bool> Metadata::co_remove(ChunkId id) {
  std::lock_guard lock(mutex_);
  auto it = chunks_.find(id);
  if (it == chunks_.end()) {
    co_return false;
  }
  freeLocked(it->second.blocks);
  chunks_.erase(it);
  co_return true;
}

size_t Metadata::freeBlocks(Tier tier) const {
  std::lock_guard lock(mutex_);
  size_t count = 0;
  for (const auto& [id, region] : regions_) {
    if (region.info.tier == tier) {
      count += region.free.size();
    }
  }
  return count;
}

size_t Metadata::chunkCount() const {
  std::lock_guard lock(mutex_);
  return chunks_.size();
}

} // namespace kiln::meta
