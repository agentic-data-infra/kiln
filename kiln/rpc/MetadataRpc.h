#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include <folly/coro/Task.h>

#include "kiln/common/Types.h"

namespace kiln::rpc {

// The metadata server's operations, independent of fbthrift. The metadata
// server implements them (kiln/meta), and connectMetadata (Rpc.h) calls them
// over the network. Calls fail with kiln::Error.
class MetadataRpc {
 public:
  virtual ~MetadataRpc() = default;

  // A storage server offers its regions (ids unset), of which it lets
  // 'budget' bytes be used (0 for all); returns them with the ids the
  // metadata server gave them.
  virtual folly::coro::Task<std::vector<RegionInfo>> co_registerServer(
      std::vector<RegionInfo> regions,
      uint64_t budget) = 0;

  virtual folly::coro::Task<std::vector<RegionInfo>> co_getRegions(
      std::vector<RegionId> ids) = 0;

  // 'count' free blocks of length kBlockSize in a tier of blocks. Evicts
  // the tier's least recently used copies of chunks to make room; fails
  // with kOutOfSpace if that cannot make enough. With 'overflow', takes the
  // blocks from 'tier' only if they are free there without evicting, else
  // from 'overflow' as above; the blocks' regions tell which.
  virtual folly::coro::Task<std::vector<BlockRef>> co_allocate(
      Tier tier,
      uint32_t count,
      std::optional<Tier> overflow = std::nullopt) = 0;

  // Returns blocks from co_allocate that no chunk uses.
  virtual folly::coro::Task<void> co_free(std::vector<BlockRef> blocks) = 0;

  // Makes a chunk visible. A chunk for a bucket that does not exist fails
  // with kNotFound, and its blocks are freed. Its replicas are its copies in the placement's
  // tier and write-through tier, in that order. The first commit of an id
  // wins: a later one gets the first one's chunk back, with its own copies
  // added in tiers where that one has none (e.g. evicted ones), and the
  // blocks of its other copies freed.
  virtual folly::coro::Task<ChunkInfo> co_commit(ChunkInfo chunk) = 0;

  // The chunk with the copies that can be read now. Fails with kNotFound
  // if there is no such chunk, e.g. it was evicted, and kUnavailable if
  // none of its copies' storage servers has registered (after a restart).
  virtual folly::coro::Task<ChunkInfo> co_lookup(ChunkId id) = 0;

  // Returns whether the chunk existed. Its blocks are freed after the grace
  // period, so readers that looked it up just before can still finish.
  virtual folly::coro::Task<bool> co_remove(ChunkId id) = 0;

  // A new bucket, for chunks to remove together (Placement::bucket). Ids
  // are never reused.
  virtual folly::coro::Task<BucketId> co_createBucket() = 0;

  // Removes the bucket and its chunks, as co_remove does, and returns
  // whether it existed. Later commits into it fail with kNotFound.
  virtual folly::coro::Task<bool> co_deleteBucket(BucketId bucket) = 0;

  // The ref, or nullopt if there is none.
  virtual folly::coro::Task<std::optional<Ref>> co_getRef(std::string name) = 0;

  // Sets the ref to 'value' if its version is 'expectedVersion', or, with
  // nullopt, if there is no such ref. Returns the updated ref, or nullopt if
  // the version did not match. Names have at most kMaxRefNameSize bytes and
  // values kMaxRefValueSize.
  virtual folly::coro::Task<std::optional<Ref>> co_casRef(
      std::string name,
      std::optional<uint64_t> expectedVersion,
      std::string value) = 0;

  // Up to 'limit' (0 for all) refs whose names start with 'prefix' and come
  // after 'startAfter' (empty for from the first), by name: to list many
  // refs in pages, start each page after the last name of the one before.
  virtual folly::coro::Task<std::vector<Ref>> co_listRefs(
      std::string prefix,
      std::string startAfter,
      uint32_t limit) = 0;

  // Removes the ref if its version is 'expectedVersion', or, with nullopt,
  // whatever its version. Returns whether it did.
  virtual folly::coro::Task<bool> co_removeRef(
      std::string name,
      std::optional<uint64_t> expectedVersion) = 0;
};

} // namespace kiln::rpc
