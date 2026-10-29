#include "kiln/rdma/MetadataProtocol.h"

#include <cstring>

#include <fmt/format.h>

namespace kiln::rdma {

size_t MetadataMessage::write(std::span<std::byte> out) const {
  if (bytes.size() <= out.size()) {
    std::memcpy(out.data(), bytes.data(), bytes.size());
  }
  return bytes.size();
}

void MetadataMessage::update(std::span<const std::byte> in) {
  bytes.assign(reinterpret_cast<const char*>(in.data()), in.size());
}

namespace {

template <typename T>
void append(std::string& bytes, T value) {
  // x86 and ARM are little endian.
  bytes.append(reinterpret_cast<const char*>(&value), sizeof(value));
}

bool isCompact(const std::vector<BlockRef>& blocks) {
  for (size_t i = 0; i < blocks.size(); ++i) {
    const auto& block = blocks[i];
    if (block.offset % kBlockSize != 0 ||
        block.offset / kBlockSize > UINT32_MAX ||
        (i + 1 < blocks.size() ? block.length != kBlockSize
                               : block.length > kBlockSize)) {
      return false;
    }
  }
  return true;
}

} // namespace

MessageWriter& MessageWriter::u8(uint8_t value) {
  append(bytes_, value);
  return *this;
}

MessageWriter& MessageWriter::u16(uint16_t value) {
  append(bytes_, value);
  return *this;
}

MessageWriter& MessageWriter::u32(uint32_t value) {
  append(bytes_, value);
  return *this;
}

MessageWriter& MessageWriter::u64(uint64_t value) {
  append(bytes_, value);
  return *this;
}

MessageWriter& MessageWriter::string(std::string_view value) {
  u32(static_cast<uint32_t>(value.size()));
  bytes_.append(value);
  return *this;
}

MessageWriter& MessageWriter::endpoint(const Endpoint& endpoint) {
  return string(endpoint.host).u16(endpoint.port);
}

MessageWriter& MessageWriter::regions(const std::vector<RegionInfo>& regions) {
  u32(static_cast<uint32_t>(regions.size()));
  for (const auto& region : regions) {
    u64(region.id).string(region.server);
    u8(static_cast<uint8_t>(region.tier)).u64(region.length);
    u8(region.tcp.has_value());
    if (region.tcp) {
      endpoint(region.tcp->endpoint);
    }
    u8(region.shm.has_value());
    if (region.shm) {
      string(region.shm->host).string(region.shm->path);
    }
    u8(region.rdma.has_value());
    if (region.rdma) {
      endpoint(region.rdma->endpoint);
      u32(region.rdma->rkey).u64(region.rdma->baseAddress);
    }
  }
  return *this;
}

MessageWriter& MessageWriter::regionIds(const std::vector<RegionId>& ids) {
  u32(static_cast<uint32_t>(ids.size()));
  for (auto id : ids) {
    u64(id);
  }
  return *this;
}

MessageWriter& MessageWriter::blocks(const std::vector<BlockRef>& blocks) {
  const bool compact = isCompact(blocks);
  u8(compact).u32(static_cast<uint32_t>(blocks.size()));
  for (const auto& block : blocks) {
    if (compact) {
      u64(block.region).u32(static_cast<uint32_t>(block.offset / kBlockSize));
    } else {
      u64(block.region).u64(block.offset).u64(block.length);
    }
  }
  if (compact && !blocks.empty()) {
    u32(static_cast<uint32_t>(blocks.back().length));
  }
  return *this;
}

MessageWriter& MessageWriter::chunk(const ChunkInfo& chunk) {
  u64(chunk.id.high).u64(chunk.id.low).u64(chunk.size);
  u8(static_cast<uint8_t>(chunk.placement.tier));
  u8(chunk.placement.writeThrough.has_value());
  if (chunk.placement.writeThrough) {
    u8(static_cast<uint8_t>(*chunk.placement.writeThrough));
  }
  u64(chunk.bucket);
  u32(static_cast<uint32_t>(chunk.replicas.size()));
  for (const auto& replica : chunk.replicas) {
    u8(static_cast<uint8_t>(replica.tier));
    blocks(replica.blocks);
  }
  return *this;
}

MessageWriter& MessageWriter::ref(const Ref& ref) {
  return string(ref.name).string(ref.value).u64(ref.version);
}

MessageWriter& MessageWriter::optionalRef(const std::optional<Ref>& value) {
  u8(value.has_value());
  if (value) {
    ref(*value);
  }
  return *this;
}

MessageWriter& MessageWriter::refs(const std::vector<Ref>& refs) {
  u32(static_cast<uint32_t>(refs.size()));
  for (const auto& r : refs) {
    ref(r);
  }
  return *this;
}

MessageWriter& MessageWriter::optionalVersion(
    const std::optional<uint64_t>& version) {
  u8(version.has_value());
  if (version) {
    u64(*version);
  }
  return *this;
}

std::string MessageWriter::finish(std::string_view what) && {
  if (bytes_.size() > kMaxMessageSize) {
    throw Error(
        ErrorCode::kTooLarge,
        fmt::format(
            "{} takes {} bytes, more than the {} of an RDMA metadata message",
            what,
            bytes_.size(),
            kMaxMessageSize));
  }
  return std::move(bytes_);
}

std::string_view MessageReader::take(size_t length) {
  if (length > bytes_.size()) {
    throw Error(ErrorCode::kInternal, "Truncated RDMA metadata message");
  }
  auto taken = bytes_.substr(0, length);
  bytes_.remove_prefix(length);
  return taken;
}

namespace {

template <typename T>
T read(std::string_view bytes) {
  T value;
  std::memcpy(&value, bytes.data(), sizeof(value));
  return value;
}

} // namespace

uint8_t MessageReader::u8() {
  return read<uint8_t>(take(1));
}

uint16_t MessageReader::u16() {
  return read<uint16_t>(take(2));
}

uint32_t MessageReader::u32() {
  return read<uint32_t>(take(4));
}

uint64_t MessageReader::u64() {
  return read<uint64_t>(take(8));
}

std::string MessageReader::string() {
  return std::string(take(u32()));
}

Endpoint MessageReader::endpoint() {
  Endpoint result;
  result.host = string();
  result.port = u16();
  return result;
}

std::vector<RegionInfo> MessageReader::regions() {
  std::vector<RegionInfo> result(u32());
  for (auto& region : result) {
    region.id = u64();
    region.server = string();
    region.tier = static_cast<Tier>(u8());
    region.length = u64();
    if (u8()) {
      region.tcp = TcpAccess{.endpoint = endpoint()};
    }
    if (u8()) {
      ShmAccess shm;
      shm.host = string();
      shm.path = string();
      region.shm = std::move(shm);
    }
    if (u8()) {
      RdmaAccess rdma;
      rdma.endpoint = endpoint();
      rdma.rkey = u32();
      rdma.baseAddress = u64();
      region.rdma = std::move(rdma);
    }
  }
  return result;
}

std::vector<RegionId> MessageReader::regionIds() {
  std::vector<RegionId> result(u32());
  for (auto& id : result) {
    id = u64();
  }
  return result;
}

std::vector<BlockRef> MessageReader::blocks() {
  const bool compact = u8();
  std::vector<BlockRef> result(u32());
  for (auto& block : result) {
    block.region = u64();
    if (compact) {
      block.offset = uint64_t{u32()} * kBlockSize;
      block.length = kBlockSize;
    } else {
      block.offset = u64();
      block.length = u64();
    }
  }
  if (compact && !result.empty()) {
    result.back().length = u32();
  }
  return result;
}

ChunkInfo MessageReader::chunk() {
  ChunkInfo result;
  result.id.high = u64();
  result.id.low = u64();
  result.size = u64();
  result.placement.tier = static_cast<Tier>(u8());
  if (u8()) {
    result.placement.writeThrough = static_cast<Tier>(u8());
  }
  result.bucket = u64();
  result.replicas.resize(u32());
  for (auto& replica : result.replicas) {
    replica.tier = static_cast<Tier>(u8());
    replica.blocks = blocks();
  }
  return result;
}

Ref MessageReader::ref() {
  Ref result;
  result.name = string();
  result.value = string();
  result.version = u64();
  return result;
}

std::optional<Ref> MessageReader::optionalRef() {
  if (!u8()) {
    return std::nullopt;
  }
  return ref();
}

std::vector<Ref> MessageReader::refs() {
  std::vector<Ref> result(u32());
  for (auto& r : result) {
    r = ref();
  }
  return result;
}

std::optional<uint64_t> MessageReader::optionalVersion() {
  if (!u8()) {
    return std::nullopt;
  }
  return u64();
}

MessageWriter okResponse() {
  MessageWriter writer;
  writer.u8(0);
  return writer;
}

std::string errorResponse(const Error& error) {
  MessageWriter writer;
  writer.u8(static_cast<uint8_t>(error.code()) + 1);
  // Room for the rest in any message.
  writer.string(std::string_view(error.what()).substr(0, 4096));
  return std::move(writer).finish("An error");
}

MessageReader readResponse(const std::string& response) {
  MessageReader reader(response);
  const auto status = reader.u8();
  if (status != 0) {
    throw Error(static_cast<ErrorCode>(status - 1), reader.string());
  }
  return reader;
}

} // namespace kiln::rdma
