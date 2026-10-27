// Encoding the metadata calls' values; needs no RDMA device.

#include <gtest/gtest.h>

#include "kiln/common/Error.h"
#include "kiln/rdma/MetadataProtocol.h"

namespace kiln::rdma {
namespace {

// The blocks of a chunk of 'size' bytes in two regions, as a writer
// allocates them.
std::vector<BlockRef> chunkBlocks(uint64_t size) {
  std::vector<BlockRef> blocks;
  for (uint64_t done = 0; done < size; done += kBlockSize) {
    const auto i = blocks.size();
    blocks.push_back(BlockRef{
        .region = 1 + i % 2,
        .offset = (1000 + i) * kBlockSize,
        .length = std::min(kBlockSize, size - done)});
  }
  return blocks;
}

ChunkInfo chunk(uint64_t size) {
  ChunkInfo info{
      .id = ChunkId{.high = 0x0123456789abcdef, .low = 42},
      .size = size,
      .placement = Placement{.tier = Tier::kDram, .writeThrough = Tier::kLocalFs}};
  info.replicas.push_back(Replica{.tier = Tier::kDram, .blocks = chunkBlocks(size)});
  info.replicas.push_back(
      Replica{.tier = Tier::kLocalFs, .blocks = chunkBlocks(size)});
  return info;
}

TEST(MetadataProtocolTest, roundTripsValues) {
  std::vector<RegionInfo> regions{
      RegionInfo{
          .id = 7,
          .server = "host:1234",
          .tier = Tier::kDram,
          .length = 1ull << 30,
          .tcp = TcpAccess{.endpoint = {"host", 1234}},
          .shm = ShmAccess{.host = "host", .path = "/dev/shm/kiln/region-0"},
          .rdma = RdmaAccess{
              .endpoint = {"10.0.0.1", 4321},
              .rkey = 0xfedcba98,
              .baseAddress = 0x7f0000000000}},
      RegionInfo{.id = 8, .server = "other:1", .tier = Tier::kLocalFs}};
  // Not compact: a short block before the last.
  std::vector<BlockRef> irregular{
      {.region = 3, .offset = 5, .length = 100},
      {.region = 4, .offset = kBlockSize, .length = kBlockSize}};
  const auto info = chunk((3 << 20) + 5);
  const Ref ref{.name = "memo/a", .value = std::string("a\0b", 3), .version = 9};

  MessageWriter writer;
  writer.u8(1)
      .u16(2)
      .u32(3)
      .u64(4)
      .string("five")
      .regions(regions)
      .regionIds({1, 2, 3})
      .blocks(irregular)
      .blocks({})
      .chunk(info)
      .optionalRef(ref)
      .optionalRef(std::nullopt)
      .refs({ref, ref})
      .optionalVersion(5)
      .optionalVersion(std::nullopt);
  const auto bytes = std::move(writer).finish("Test");

  MessageReader reader(bytes);
  EXPECT_EQ(reader.u8(), 1);
  EXPECT_EQ(reader.u16(), 2);
  EXPECT_EQ(reader.u32(), 3);
  EXPECT_EQ(reader.u64(), 4);
  EXPECT_EQ(reader.string(), "five");
  EXPECT_EQ(reader.regions(), regions);
  EXPECT_EQ(reader.regionIds(), (std::vector<RegionId>{1, 2, 3}));
  EXPECT_EQ(reader.blocks(), irregular);
  EXPECT_EQ(reader.blocks(), std::vector<BlockRef>{});
  EXPECT_EQ(reader.chunk(), info);
  EXPECT_EQ(reader.optionalRef(), ref);
  EXPECT_EQ(reader.optionalRef(), std::nullopt);
  EXPECT_EQ(reader.refs(), (std::vector<Ref>{ref, ref}));
  EXPECT_EQ(reader.optionalVersion(), 5);
  EXPECT_EQ(reader.optionalVersion(), std::nullopt);
  try {
    reader.u8();
    FAIL() << "Read past the end";
  } catch (const Error& e) {
    EXPECT_EQ(e.code(), ErrorCode::kInternal);
  }
}

// A chunk's blocks take 12 bytes each, so a 1 GiB chunk written through
// to a second tier fits a message; one of 3 GiB does not.
TEST(MetadataProtocolTest, chunksAreCompact) {
  MessageWriter writer;
  writer.chunk(chunk(1ull << 30));
  const auto bytes = std::move(writer).finish("A 1 GiB chunk");
  EXPECT_LT(bytes.size(), 2 * 1024 * 12 + 100);

  MessageWriter large;
  large.chunk(chunk(3ull << 30));
  try {
    std::move(large).finish("A 3 GiB chunk");
    FAIL() << "Fits a message";
  } catch (const Error& e) {
    EXPECT_EQ(e.code(), ErrorCode::kTooLarge);
  }
}

TEST(MetadataProtocolTest, carriesErrors) {
  auto ok = okResponse();
  ok.u64(17);
  const auto okBytes = std::move(ok).finish("Response");
  EXPECT_EQ(readResponse(okBytes).u64(), 17);

  const auto failed = errorResponse(Error(ErrorCode::kNotFound, "No chunk"));
  try {
    readResponse(failed);
    FAIL() << "No error";
  } catch (const Error& e) {
    EXPECT_EQ(e.code(), ErrorCode::kNotFound);
    EXPECT_STREQ(e.what(), "No chunk");
  }
}

} // namespace
} // namespace kiln::rdma
