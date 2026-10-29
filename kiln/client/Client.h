#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <folly/coro/Task.h>

#include "kiln/common/Buffer.h"
#include "kiln/common/ChunkId.h"
#include "kiln/common/Types.h"

namespace kiln::rpc {
class MetadataRpc;
}

namespace kiln::objectstore {
class ObjectStore;
}

namespace kiln::client {

class Transport;
class RdmaMemory;
struct RdmaStats;

struct ClientOptions {
  // The metadata server's URLs (MetadataAddress). With an rdma:// one, and
  // RDMA usable here (a -c kiln.rdma=true build, an RDMA device), all
  // metadata calls go over RDMA, failing with kUnavailable rather than
  // taking TCP if it cannot be reached; else all go over TCP.
  MetadataAddress metadata;
  // Make all metadata calls over TCP, even with an rdma:// URL.
  bool metaForceTcp{false};
  // Move all chunk data over TCP: no shared memory, no RDMA. Without it,
  // the client reaches each region by mapping its file if it is on this
  // host, else with one-sided RDMA if the region offers it (RdmaAccess, from
  // kiln_server --rdma) and RDMA is usable here, else over TCP.
  bool dataForceTcp{false};
  // Reach regions on this host by mapping their files; off to use RDMA or
  // TCP even there, e.g. to test them on one host.
  bool useShm{true};
  // How long a chunk's info is used without asking the metadata server
  // again. Must be well below the metadata server's grace period
  // (MetadataOptions::gracePeriod), after which the blocks of an evicted or
  // removed chunk are reused.
  std::chrono::milliseconds chunkCacheTtl{std::chrono::seconds(10)};
  // The deployment's object store, to write chunks through to it and read
  // their copies there; none if unset.
  std::shared_ptr<objectstore::ObjectStore> objectStore;
  // The RDMA device to use: that of this local address, or, if empty, of
  // the first interface that has one. With RDMA, chunk data moves straight
  // between the storage servers and buffers from allocateBuffer or
  // registerMemory; other memory goes through a copy.
  std::string rdmaHost;
};

struct ClientStats {
  // Bytes moved with RDMA straight from and to registered buffers.
  uint64_t rdmaBytes{0};
  // Bytes moved with RDMA through a copy, as the caller's memory was not
  // registered.
  uint64_t rdmaCopiedBytes{0};
  // Whether the metadata calls go over RDMA.
  bool rdmaMetadata{false};
  // Whether regions that offer RDMA are reached with it.
  bool rdmaData{false};
};

class ChunkWriter;

// Kiln's client: writes and reads chunks. It asks the metadata server for
// blocks and where chunks are, and moves the bytes itself, directly to and
// from the storage servers' regions. Chunks are immutable and named by their
// content (ChunkId). Thread safe; calls fail with kiln::Error.
class Client {
 public:
  // Connects on the first call.
  explicit Client(ClientOptions options);

  ~Client();

  Client(const Client&) = delete;
  Client& operator=(const Client&) = delete;

  // Memory to write chunks from and read them into; with RDMA, registered
  // with the NIC until the buffer goes. Registering takes time: keep buffers
  // and reuse them.
  Buffer allocateBuffer(size_t capacity);

  // The caller's 'length' bytes at 'data' as a Buffer, registered like
  // allocateBuffer's; the caller keeps them alive while the buffer is in
  // use.
  Buffer registerMemory(void* data, size_t length);

  ClientStats stats() const;

  // Starts a new chunk in the placement's tier, and its write-through
  // tier if set. With 'sizeHint', the chunk's expected size, the first
  // append allocates blocks for that many bytes at once, so that a
  // placement with an overflow tier puts the whole chunk into one tier.
  // Blocks beyond the committed size are freed.
  folly::coro::Task<std::unique_ptr<ChunkWriter>> co_create(
      Placement placement = {},
      uint64_t sizeHint = 0);

  // Reads up to 'length' bytes of the chunk at 'offset' into 'dst', which
  // must have the capacity; returns it with its size set to the bytes read
  // (fewer at the end of the chunk). Reads the fastest copy it can reach.
  // Fails with kNotFound for no such chunk and kInvalidArgument for an
  // offset past its end.
  folly::coro::Task<Buffer>
  co_read(ChunkId id, uint64_t offset, uint64_t length, Buffer&& dst);

  // The chunk's size and copies. Fails with kNotFound for no such chunk.
  folly::coro::Task<ChunkInfo> co_stat(ChunkId id);

  // Returns whether the chunk existed. Its object in the object store, if
  // any, stays.
  folly::coro::Task<bool> co_remove(ChunkId id);

  // A new bucket, for chunks to remove together (see Placement::bucket).
  folly::coro::Task<BucketId> co_createBucket();

  // Removes the bucket and its chunks, as co_remove does; returns whether
  // it existed.
  folly::coro::Task<bool> co_deleteBucket(BucketId bucket);

  // Refs: small named values on the metadata server, updated with compare
  // and swap (see Ref and rpc::MetadataRpc).

  // The ref, or nullopt if there is none.
  folly::coro::Task<std::optional<Ref>> co_getRef(std::string name);

  // Sets the ref to 'value' if its version is 'expectedVersion', or, with
  // nullopt, if there is no such ref. Returns the updated ref, or nullopt if
  // the version did not match.
  folly::coro::Task<std::optional<Ref>> co_casRef(
      std::string name,
      std::optional<uint64_t> expectedVersion,
      std::string value);

  // Up to 'limit' (0 for all) refs whose names start with 'prefix' and come
  // after 'startAfter' (empty for from the first), by name. All of them are
  // asked for in pages, which fit the metadata server's RDMA messages.
  folly::coro::Task<std::vector<Ref>> co_listRefs(
      std::string prefix,
      std::string startAfter = {},
      uint32_t limit = 0);

  // Removes the ref if its version is 'expectedVersion', or, with nullopt,
  // whatever its version. Returns whether it did.
  folly::coro::Task<bool> co_removeRef(
      std::string name,
      std::optional<uint64_t> expectedVersion = std::nullopt);

 private:
  friend class ChunkWriter;

  // Reads 'length' bytes of the chunk's copy at 'offset' into 'dst'.
  folly::coro::Task<void> co_readReplica(
      ChunkId id,
      const Replica& replica,
      uint64_t offset,
      uint64_t length,
      uint8_t* dst);

  // The regions of 'blocks', from the cache or the metadata server.
  folly::coro::Task<std::vector<RegionInfo>> co_regions(
      const std::vector<BlockRef>& blocks);

  Transport& transportFor(const RegionInfo& region);

  // Registers the buffer's memory with the NIC if RDMA is in use; if that
  // fails, the buffer goes through a copy.
  void registerWithNic(Buffer& buffer);

  void cacheChunk(const ChunkInfo& chunk);

  const ClientOptions options_;
  std::shared_ptr<rpc::MetadataRpc> metadata_;
  bool rdmaMetadata_{false};
  const std::unique_ptr<Transport> tcp_;
  const std::unique_ptr<Transport> shm_;
  // Set if RDMA is usable for data.
  std::shared_ptr<RdmaMemory> rdmaMemory_;
  std::shared_ptr<RdmaStats> rdmaStats_;
  std::unique_ptr<Transport> rdma_;

  std::mutex mutex_;
  std::unordered_map<RegionId, RegionInfo> regions_;
  struct CachedChunk {
    ChunkInfo info;
    std::chrono::steady_clock::time_point expires;
  };

  // Chunks are immutable, so their infos are valid until removed or
  // evicted; lookups after the TTL also tell the metadata server which
  // chunks are in use.
  std::unordered_map<ChunkId, CachedChunk> chunks_;
};

// Writes one chunk: append its bytes, then commit to get its id. The bytes
// go to the storage servers as they are appended, to both tiers when
// writing through to local FS, and are hashed on the way; nobody sees the
// chunk before it is committed. Writing through to the object store, commit
// reads the chunk back and puts it there first. If a chunk with the same
// bytes exists already in the same bucket (or none), commit returns its id
// and this copy is dropped.
// Not thread safe: one append at a time.
class ChunkWriter {
 public:
  // Without co_commit or co_abort the blocks stay allocated.
  ~ChunkWriter();

  // Appends data.bytes() and returns 'data' for reuse.
  folly::coro::Task<Buffer> co_append(Buffer&& data);

  // Makes the chunk visible and returns its id.
  folly::coro::Task<ChunkId> co_commit();

  // Drops the chunk and frees its blocks.
  folly::coro::Task<void> co_abort();

  // The bytes appended so far.
  uint64_t size() const {
    return size_;
  }

 private:
  friend class Client;

  ChunkWriter(Client& client, Placement placement, uint64_t sizeHint);

  void checkOpen() const;

  // Appends 'bytes' to a copy in a tier of blocks.
  folly::coro::Task<void> co_appendTo(Replica& replica, folly::ByteRange bytes);

  Client& client_;
  // The overflow tier is resolved by the first allocation.
  Placement placement_;
  const uint64_t sizeHint_;
  std::unique_ptr<ChunkHasher> hasher_;
  // The copies in tiers of blocks, by tier.
  std::vector<Replica> replicas_;
  uint64_t size_{0};
  // Committed, aborted or failed.
  bool closed_{false};
};

} // namespace kiln::client
