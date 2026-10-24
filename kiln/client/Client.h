#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include <folly/coro/Task.h>

#include "kiln/common/Buffer.h"
#include "kiln/common/ChunkId.h"
#include "kiln/common/Types.h"

namespace kiln::rpc {
class MetadataRpc;
}

namespace kiln::client {

class Transport;

struct ClientOptions {
  // The metadata server.
  Endpoint metadata;
  // Reach regions on this host by mapping their files instead of over TCP.
  // Off to force TCP, e.g. to test it on one host.
  bool useShm{true};
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

  // Memory to write chunks from and read them into.
  Buffer allocateBuffer(size_t capacity);

  // The caller's 'length' bytes at 'data' as a Buffer; the caller keeps them
  // alive while the buffer is in use.
  Buffer registerMemory(void* data, size_t length);

  // Starts a new chunk in the placement's tier.
  folly::coro::Task<std::unique_ptr<ChunkWriter>> co_create(
      Placement placement = {});

  // Reads up to 'length' bytes of the chunk at 'offset' into 'dst', which
  // must have the capacity; returns it with its size set to the bytes read
  // (fewer at the end of the chunk). Fails with kNotFound for no such chunk
  // and kInvalidArgument for an offset past its end.
  folly::coro::Task<Buffer>
  co_read(ChunkId id, uint64_t offset, uint64_t length, Buffer&& dst);

  // The chunk's size and blocks. Fails with kNotFound for no such chunk.
  folly::coro::Task<ChunkInfo> co_stat(ChunkId id);

  // Returns whether the chunk existed.
  folly::coro::Task<bool> co_remove(ChunkId id);

 private:
  friend class ChunkWriter;

  // The regions of 'blocks', from the cache or the metadata server.
  folly::coro::Task<std::vector<RegionInfo>> co_regions(
      const std::vector<BlockRef>& blocks);

  Transport& transportFor(const RegionInfo& region);

  void cacheChunk(const ChunkInfo& chunk);

  const ClientOptions options_;
  const std::shared_ptr<rpc::MetadataRpc> metadata_;
  const std::unique_ptr<Transport> tcp_;
  const std::unique_ptr<Transport> shm_;

  std::mutex mutex_;
  std::unordered_map<RegionId, RegionInfo> regions_;
  // Chunks are immutable, so their infos stay valid until removed.
  std::unordered_map<ChunkId, ChunkInfo> chunks_;
};

// Writes one chunk: append its bytes, then commit to get its id. The bytes
// go to the storage servers as they are appended and are hashed on the way;
// nobody sees the chunk before it is committed. If a chunk with the same
// bytes exists already, commit returns its id and this copy is dropped.
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

  ChunkWriter(Client& client, Placement placement);

  void checkOpen() const;

  Client& client_;
  const Placement placement_;
  std::unique_ptr<ChunkHasher> hasher_;
  std::vector<BlockRef> blocks_;
  uint64_t size_{0};
  // Committed, aborted or failed.
  bool closed_{false};
};

} // namespace kiln::client
