// The objects Kiln's metadata describes. Storage servers hold blocks inside
// large regions of memory or file; the metadata server knows the regions and
// which blocks make up each chunk; clients read and write blocks directly at
// the servers, at (region, offset, length), so that with RDMA (later) servers
// spend no CPU on data.

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

// This machine's host name, as other hosts should reach it.
std::string localHostName();

// Where blocks live. Both tiers are ephemeral: their data is gone when the
// storage server stops.
enum class Tier {
  // Memory, shared through a file in /dev/shm.
  kDram,
  // A preallocated file on the storage server's local file system.
  kLocalFs,
};

const char* toString(Tier tier);

// Parses "dram" or "localfs". Throws Error(kInvalidArgument) otherwise.
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

// Reaching a region with one-sided RDMA. Set only if the region is
// registered with the NIC; not implemented yet.
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

// Where a new chunk goes.
struct Placement {
  Tier tier{Tier::kDram};

  bool operator==(const Placement&) const = default;
};

// A committed chunk: its 'size' bytes are the blocks' bytes in order.
struct ChunkInfo {
  ChunkId id;
  uint64_t size{0};
  std::vector<BlockRef> blocks;
  Placement placement;

  bool operator==(const ChunkInfo&) const = default;
};

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
