// The metadata calls (rpc::MetadataRpc) as messages of rlib's RPC: each call
// one request and one response of at most kMaxMessageSize bytes. Built only
// with -c kiln.rdma=true.

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "kiln/common/Error.h"
#include "kiln/common/Types.h"

namespace kiln::rdma {

// The most bytes a request or response takes. Every connection keeps
// 2 * RdmaRpcOptions::recvQueue messages of it in registered memory on each
// side. A chunk's ChunkInfo takes about 12 bytes per block per copy, so a
// chunk of 1 GiB written through to a second tier takes 25 KiB.
constexpr size_t kMaxMessageSize = 64 << 10;

enum class MetadataOp : uint8_t {
  kRegisterServer = 1,
  kGetRegions,
  kAllocate,
  kFree,
  kCommit,
  kLookup,
  kRemove,
  kGetRef,
  kCasRef,
  kListRefs,
  kRemoveRef,
  kCreateBucket,
  kDeleteBucket,
};

// A request's or response's bytes, as rlib's RPC sends them.
struct MetadataMessage {
  static constexpr size_t kSize = kMaxMessageSize;

  std::string bytes;

  size_t write(std::span<std::byte> out) const;
  void update(std::span<const std::byte> in);
};

struct MetadataProtocol {
  using Request = MetadataMessage;
  using Response = MetadataMessage;
};

// Appends values to a message: integers little endian, strings and lists
// with a u32 length first.
class MessageWriter {
 public:
  MessageWriter& u8(uint8_t value);
  MessageWriter& u16(uint16_t value);
  MessageWriter& u32(uint32_t value);
  MessageWriter& u64(uint64_t value);
  MessageWriter& string(std::string_view value);
  MessageWriter& endpoint(const Endpoint& endpoint);
  MessageWriter& regions(const std::vector<RegionInfo>& regions);
  MessageWriter& regionIds(const std::vector<RegionId>& ids);
  // A list of blocks; compact (12 bytes a block) if all are whole blocks at
  // block offsets but the last, which may be shorter, as in a chunk's copy.
  MessageWriter& blocks(const std::vector<BlockRef>& blocks);
  MessageWriter& chunk(const ChunkInfo& chunk);
  MessageWriter& ref(const Ref& ref);
  MessageWriter& optionalRef(const std::optional<Ref>& ref);
  MessageWriter& refs(const std::vector<Ref>& refs);
  MessageWriter& optionalVersion(const std::optional<uint64_t>& version);

  // The message; throws Error(kTooLarge) if it exceeds kMaxMessageSize.
  std::string finish(std::string_view what) &&;

 private:
  std::string bytes_;
};

// Reads what a MessageWriter wrote; throws Error(kInternal) if the message
// ends early.
class MessageReader {
 public:
  explicit MessageReader(std::string_view bytes) : bytes_{bytes} {}

  uint8_t u8();
  uint16_t u16();
  uint32_t u32();
  uint64_t u64();
  std::string string();
  Endpoint endpoint();
  std::vector<RegionInfo> regions();
  std::vector<RegionId> regionIds();
  std::vector<BlockRef> blocks();
  ChunkInfo chunk();
  Ref ref();
  std::optional<Ref> optionalRef();
  std::vector<Ref> refs();
  std::optional<uint64_t> optionalVersion();

 private:
  std::string_view take(size_t length);

  std::string_view bytes_;
};

// A response: a status, then, if 0, the result, else the error's code + 1
// and message.
MessageWriter okResponse();
std::string errorResponse(const Error& error);

// The reader of a response's result; throws the response's error if it has
// one.
MessageReader readResponse(const std::string& response);

} // namespace kiln::rdma
