#include "kiln/rpc/Convert.h"

namespace kiln::rpc {

thrift::Endpoint toThrift(const Endpoint& endpoint) {
  thrift::Endpoint result;
  result.host() = endpoint.host;
  result.port() = endpoint.port;
  return result;
}

Endpoint fromThrift(const thrift::Endpoint& endpoint) {
  return Endpoint{
      .host = *endpoint.host(),
      .port = static_cast<uint16_t>(*endpoint.port())};
}

thrift::RegionInfo toThrift(const RegionInfo& region) {
  thrift::RegionInfo result;
  result.id() = static_cast<int64_t>(region.id);
  result.server() = region.server;
  result.tier() = static_cast<thrift::Tier>(region.tier);
  result.length() = static_cast<int64_t>(region.length);
  if (region.tcp) {
    thrift::TcpAccess tcp;
    tcp.endpoint() = toThrift(region.tcp->endpoint);
    result.tcp() = std::move(tcp);
  }
  if (region.shm) {
    thrift::ShmAccess shm;
    shm.host() = region.shm->host;
    shm.path() = region.shm->path;
    result.shm() = std::move(shm);
  }
  if (region.rdma) {
    thrift::RdmaAccess rdma;
    rdma.endpoint() = toThrift(region.rdma->endpoint);
    rdma.rkey() = region.rdma->rkey;
    rdma.baseAddress() = static_cast<int64_t>(region.rdma->baseAddress);
    result.rdma() = std::move(rdma);
  }
  return result;
}

RegionInfo fromThrift(const thrift::RegionInfo& region) {
  RegionInfo result{
      .id = static_cast<RegionId>(*region.id()),
      .server = *region.server(),
      .tier = static_cast<Tier>(*region.tier()),
      .length = static_cast<uint64_t>(*region.length())};
  if (region.tcp()) {
    result.tcp = TcpAccess{.endpoint = fromThrift(*region.tcp()->endpoint())};
  }
  if (region.shm()) {
    result.shm =
        ShmAccess{.host = *region.shm()->host(), .path = *region.shm()->path()};
  }
  if (region.rdma()) {
    result.rdma = RdmaAccess{
        .endpoint = fromThrift(*region.rdma()->endpoint()),
        .rkey = static_cast<uint32_t>(*region.rdma()->rkey()),
        .baseAddress = static_cast<uint64_t>(*region.rdma()->baseAddress())};
  }
  return result;
}

thrift::BlockRef toThrift(const BlockRef& block) {
  thrift::BlockRef result;
  result.region() = static_cast<int64_t>(block.region);
  result.offset() = static_cast<int64_t>(block.offset);
  result.length() = static_cast<int64_t>(block.length);
  return result;
}

BlockRef fromThrift(const thrift::BlockRef& block) {
  return BlockRef{
      .region = static_cast<RegionId>(*block.region()),
      .offset = static_cast<uint64_t>(*block.offset()),
      .length = static_cast<uint64_t>(*block.length())};
}

thrift::ChunkId toThrift(const ChunkId& id) {
  thrift::ChunkId result;
  result.high() = static_cast<int64_t>(id.high);
  result.low() = static_cast<int64_t>(id.low);
  return result;
}

ChunkId fromThrift(const thrift::ChunkId& id) {
  return ChunkId{
      .high = static_cast<uint64_t>(*id.high()),
      .low = static_cast<uint64_t>(*id.low())};
}

thrift::Placement toThrift(const Placement& placement) {
  thrift::Placement result;
  result.tier() = static_cast<thrift::Tier>(placement.tier);
  return result;
}

Placement fromThrift(const thrift::Placement& placement) {
  return Placement{.tier = static_cast<Tier>(*placement.tier())};
}

thrift::ChunkInfo toThrift(const ChunkInfo& chunk) {
  thrift::ChunkInfo result;
  result.id() = toThrift(chunk.id);
  result.size() = static_cast<int64_t>(chunk.size);
  result.blocks() = toThrift(chunk.blocks);
  result.placement() = toThrift(chunk.placement);
  return result;
}

ChunkInfo fromThrift(const thrift::ChunkInfo& chunk) {
  return ChunkInfo{
      .id = fromThrift(*chunk.id()),
      .size = static_cast<uint64_t>(*chunk.size()),
      .blocks = fromThrift(*chunk.blocks()),
      .placement = fromThrift(*chunk.placement())};
}

thrift::KilnException toThrift(const Error& error) {
  thrift::KilnException result;
  result.code() = static_cast<thrift::ErrorCode>(error.code());
  result.message() = error.what();
  return result;
}

Error fromThrift(const thrift::KilnException& error) {
  return Error(static_cast<ErrorCode>(*error.code()), *error.message());
}

} // namespace kiln::rpc
