// Kiln's services: the metadata server's (MetadataService) and the storage
// servers' (StorageService). Compiled by thrift1 into kiln/rpc/gen-cpp2 (see
// BUCK). The types mirror kiln/common/Types.h.

package "kiln.dev/rpc"

namespace cpp2 kiln.rpc.thrift

include "thrift/annotation/cpp.thrift"
include "thrift/annotation/thrift.thrift"

/// Bytes received into, and sent from, an IOBuf without copying.
@cpp.Type{name = "folly::IOBuf"}
typedef binary IOBuf

/// kiln::ErrorCode.
enum ErrorCode {
  NOT_FOUND = 0,
  OUT_OF_SPACE = 1,
  INVALID_ARGUMENT = 2,
  UNAVAILABLE = 3,
  INTERNAL = 4,
  TOO_LARGE = 5,
}

/// A kiln::Error thrown by a handler.
exception KilnException {
  1: ErrorCode code;
  @thrift.ExceptionMessage
  2: string message;
}

enum Tier {
  DRAM = 0,
  LOCAL_FS = 1,
  OBJECT_STORE = 2,
}

struct Endpoint {
  1: string host;
  2: i32 port;
}

struct TcpAccess {
  1: Endpoint endpoint;
}

struct ShmAccess {
  1: string host;
  2: string path;
}

struct RdmaAccess {
  1: Endpoint endpoint;
  /// A u32; i64 as thrift has no unsigned types.
  2: i64 rkey;
  3: i64 baseAddress;
}

struct RegionInfo {
  1: i64 id;
  2: string server;
  3: Tier tier;
  4: i64 length;
  5: optional TcpAccess tcp;
  6: optional ShmAccess shm;
  7: optional RdmaAccess rdma;
}

struct BlockRef {
  1: i64 region;
  2: i64 offset;
  3: i64 length;
}

struct ChunkId {
  1: i64 high;
  2: i64 low;
}

struct Placement {
  1: Tier tier;
  2: optional Tier writeThrough;
}

struct Replica {
  1: Tier tier;
  2: list<BlockRef> blocks;
}

struct ChunkInfo {
  1: ChunkId id;
  2: i64 size;
  3: Placement placement;
  4: list<Replica> replicas;
  /// 0 for the default bucket.
  5: i64 bucket;
}

struct Ref {
  1: string name;
  2: binary value;
  3: i64 version;
}

/// A ref, if there is one.
struct RefLookup {
  1: optional Ref ref;
}

struct CasRefRequest {
  1: string name;
  /// Unset: only if there is no such ref.
  2: optional i64 expectedVersion;
  3: binary value;
}

struct RemoveRefRequest {
  1: string name;
  /// Unset: whatever the ref's version.
  2: optional i64 expectedVersion;
}

service MetadataService {
  /// Returns 'message'.
  string ping(1: string message);

  /// A storage server offers its regions, of which it lets 'budget' bytes
  /// be used (0 for all); returns them with their ids.
  list<RegionInfo> registerServer(
    1: list<RegionInfo> regions,
    2: i64 budget,
  ) throws (1: KilnException e);

  list<RegionInfo> getRegions(1: list<i64> ids) throws (1: KilnException e);

  /// 'count' free blocks of length kBlockSize in the tier, evicting the
  /// least recently used chunks there if needed. With an overflow tier (a
  /// list of at most one), only blocks free without evicting, else blocks
  /// of the overflow tier.
  list<BlockRef> allocate(
    1: Tier tier,
    2: i32 count,
    3: list<Tier> overflow,
  ) throws (
    1: KilnException e,
  );

  /// Returns blocks that allocate() handed out to the free lists.
  void free(1: list<BlockRef> blocks) throws (1: KilnException e);

  /// Makes a chunk visible, in its bucket, which must exist. The first commit of an id wins: later ones get
  /// the first one's chunk, with their copies in tiers it has no copy in
  /// added, and the blocks of their other copies are freed.
  ChunkInfo commit(1: ChunkInfo chunk) throws (1: KilnException e);

  /// The chunk with its copies that can be read now.
  ChunkInfo lookup(1: ChunkId id) throws (1: KilnException e);

  /// Returns whether the chunk existed. Its blocks are freed after the grace
  /// period.
  bool remove(1: ChunkId id) throws (1: KilnException e);

  /// A new bucket's id.
  i64 createBucket() throws (1: KilnException e);

  /// Removes the bucket and its chunks, whose blocks are reusable after
  /// 'graceMs'; returns whether it existed.
  bool deleteBucket(1: i64 bucket, 2: i64 graceMs) throws (
    1: KilnException e,
  );

  RefLookup getRef(1: string name) throws (1: KilnException e);

  /// Sets the ref if its version is the expected one; returns the updated
  /// ref, or none if the version did not match.
  RefLookup casRef(1: CasRefRequest request) throws (1: KilnException e);

  /// Up to 'limit' (0 for all) refs whose names start with 'prefix' and
  /// come after 'startAfter', by name.
  list<Ref> listRefs(
    1: string prefix,
    2: string startAfter,
    3: i32 limit,
  ) throws (1: KilnException e);

  /// Returns whether the ref was removed.
  bool removeRef(1: RemoveRefRequest request) throws (1: KilnException e);
}

service StorageService {
  IOBuf read(1: i64 region, 2: i64 offset, 3: i64 length) throws (
    1: KilnException e,
  );

  void write(1: i64 region, 2: i64 offset, 3: IOBuf data) throws (
    1: KilnException e,
  );
}
