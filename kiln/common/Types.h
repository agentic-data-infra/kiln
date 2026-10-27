// The objects Kiln's metadata describes. Storage servers hold blocks inside
// large regions of memory or file; the metadata server knows the regions and
// which blocks make up each chunk; clients read and write blocks directly at
// the servers, at (region, offset, length), so that with RDMA servers spend
// no CPU on data.

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "kiln/common/ChunkId.h"

namespace kiln {

// A chunk's data is stored in blocks of this size (the last one may be
// shorter), inside regions of (by default) this size.
constexpr uint64_t kBlockSize = 1 << 20;
constexpr uint64_t kRegionSize = 1ull << 30;

// A TCP endpoint, written "host:port" ("[v6addr]:port" for IPv6 literals).
struct Endpoint {
  std::string host;
  uint16_t port{0};

  std::string toString() const;

  // Parses "host:port". Throws Error(kInvalidArgument) if malformed.
  static Endpoint parse(const std::string& endpoint);

  bool operator==(const Endpoint&) const = default;
};

// Where a metadata server is reached: over TCP, and over RDMA if it serves
// the metadata calls there too (kiln_meta --rdma). Written as a comma
// separated list of URLs, "tcp://host:port,rdma://host:port", in any order;
// a bare "host:port" is a tcp:// URL. The rdma:// host must be an address
// on the server's RDMA interface.
struct MetadataAddress {
  std::optional<Endpoint> tcp;
  std::optional<Endpoint> rdma;

  MetadataAddress() = default;

  // Just TCP.
  MetadataAddress(Endpoint endpoint) : tcp{std::move(endpoint)} {}

  std::string toString() const;

  // Throws Error(kInvalidArgument) if malformed, empty, or with a scheme
  // twice.
  static MetadataAddress parse(const std::string& urls);

  bool operator==(const MetadataAddress&) const = default;
};

// This machine's host name, as other hosts should reach it.
std::string localHostName();

// Where a chunk's bytes live.
enum class Tier {
  // Blocks in memory, shared through files in /dev/shm. Gone when the
  // storage server stops.
  kDram,
  // Blocks in preallocated files on the storage servers' local file
  // systems. A storage server with its own directory keeps them across
  // restarts.
  kLocalFs,
  // Whole chunks as objects in an object store (kiln/objectstore), at
  // chunkObjectKey(id). Only a write-through target.
  kObjectStore,
};

// The tiers of blocks, which storage servers hold.
constexpr size_t kBlockTiers = 2;

inline bool hasBlocks(Tier tier) {
  return tier != Tier::kObjectStore;
}

const char* toString(Tier tier);

// Parses "dram", "localfs" or "objectstore". Throws Error(kInvalidArgument)
// otherwise.
Tier parseTier(const std::string& name);

using RegionId = uint64_t;

// Reaching a region with the storage server's TCP service.
struct TcpAccess {
  Endpoint endpoint;

  bool operator==(const TcpAccess&) const = default;
};

// Reaching a region from the same host by mapping its file.
struct ShmAccess {
  std::string host;
  std::string path;

  bool operator==(const ShmAccess&) const = default;
};

// Reaching a region with one-sided RDMA: its bytes are at
// [baseAddress, baseAddress + length) under 'rkey' at the storage server
// listening for RDMA connections at 'endpoint'. Set only for DRAM regions of
// storage servers that serve RDMA (StorageServerOptions::rdma).
struct RdmaAccess {
  Endpoint endpoint;
  uint32_t rkey{0};
  uint64_t baseAddress{0};

  bool operator==(const RdmaAccess&) const = default;
};

// A storage server's region and the ways to reach it.
struct RegionInfo {
  RegionId id{0};
  // The storage server's endpoint, as "host:port".
  std::string server;
  Tier tier{Tier::kDram};
  uint64_t length{0};
  std::optional<TcpAccess> tcp;
  std::optional<ShmAccess> shm;
  std::optional<RdmaAccess> rdma;

  bool operator==(const RegionInfo&) const = default;
};

// 'length' bytes at 'offset' in a region. A block's offset is a multiple of
// kBlockSize and its length at most kBlockSize.
struct BlockRef {
  RegionId region{0};
  uint64_t offset{0};
  uint64_t length{0};

  bool operator==(const BlockRef&) const = default;
};

// Where a new chunk goes: into 'tier' (DRAM or local FS), and, if set, also
// into 'writeThrough' before its commit returns. The metadata server
// journals a written-through chunk's copy in its target tier, so with a
// local FS or object store target, the chunk outlives restarts.
struct Placement {
  Tier tier{Tier::kDram};
  std::optional<Tier> writeThrough;

  bool operator==(const Placement&) const = default;
};

// A copy of a chunk in one tier: its bytes are the blocks' bytes in order,
// or, in the object store, the object at chunkObjectKey(id).
struct Replica {
  Tier tier{Tier::kDram};
  std::vector<BlockRef> blocks;

  bool operator==(const Replica&) const = default;
};

// A committed chunk and its copies, fastest tier first. Eviction drops a
// copy in one tier; the chunk is gone when it has none left.
struct ChunkInfo {
  ChunkId id;
  uint64_t size{0};
  Placement placement;
  std::vector<Replica> replicas;

  bool operator==(const ChunkInfo&) const = default;
};

// Where an object store keeps a chunk: "chunks/<id>".
std::string chunkObjectKey(const ChunkId& id);

// A small named value with a version, kept by the metadata server, e.g. a
// pointer to the chunk that holds a catalog's latest commit. Updates are
// compare-and-swap on the version, which grows with every update of any
// ref, so a removed and recreated ref never repeats a version.
struct Ref {
  std::string name;
  std::string value;
  uint64_t version{0};

  bool operator==(const Ref&) const = default;
};

constexpr size_t kMaxRefNameSize = 256;
constexpr size_t kMaxRefValueSize = 1024;

} // namespace kiln
