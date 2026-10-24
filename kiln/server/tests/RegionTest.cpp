#include <gtest/gtest.h>

#include <filesystem>

#include "kiln/common/Error.h"
#include "kiln/server/Region.h"

namespace kiln::server {
namespace {

class RegionTest : public testing::TestWithParam<Tier> {
 protected:
  void SetUp() override {
    dir_ = std::filesystem::path(
               GetParam() == Tier::kDram ? "/dev/shm"
                                         : testing::TempDir()) /
        fmt::format("kiln-region-test-{}", ::getpid());
    std::filesystem::create_directories(dir_);
  }

  void TearDown() override {
    std::filesystem::remove_all(dir_);
  }

  std::filesystem::path dir_;
};

TEST_P(RegionTest, readWrite) {
  const auto path = (dir_ / "region").string();
  {
    Region region(GetParam(), path, 4 * kBlockSize);
    EXPECT_TRUE(std::filesystem::exists(path));
    EXPECT_EQ(std::filesystem::file_size(path), 4 * kBlockSize);

    // A chained IOBuf lands contiguously.
    auto data = folly::IOBuf::copyBuffer("hello ");
    data->appendToChain(folly::IOBuf::copyBuffer("region"));
    region.write(kBlockSize - 3, *data);
    EXPECT_EQ(
        region.read(kBlockSize - 3, 12)->moveToFbString().toStdString(),
        "hello region");
    // Never written bytes are zero.
    EXPECT_EQ(
        region.read(0, 4)->moveToFbString().toStdString(),
        std::string(4, '\0'));
    EXPECT_EQ(region.read(4 * kBlockSize, 0)->computeChainDataLength(), 0);

    for (auto [offset, length] : std::vector<std::pair<uint64_t, uint64_t>>{
             {4 * kBlockSize, 1}, {4 * kBlockSize - 1, 2}, {~0ull, 2}}) {
      try {
        region.read(offset, length);
        FAIL() << "Read outside the region";
      } catch (const Error& e) {
        EXPECT_EQ(e.code(), ErrorCode::kInvalidArgument);
      }
    }
  }
  // Removed with the region.
  EXPECT_FALSE(std::filesystem::exists(path));
}

INSTANTIATE_TEST_SUITE_P(
    Tiers,
    RegionTest,
    testing::Values(Tier::kDram, Tier::kLocalFs),
    [](const auto& info) { return toString(info.param); });

} // namespace
} // namespace kiln::server
