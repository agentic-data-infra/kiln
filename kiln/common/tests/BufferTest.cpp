#include <gtest/gtest.h>

#include "kiln/common/Buffer.h"

namespace kiln {
namespace {

TEST(BufferTest, allocate) {
  auto buffer = Buffer::allocate(10'000);
  EXPECT_EQ(buffer.capacity(), 10'000);
  EXPECT_EQ(buffer.size(), 0);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(buffer.data()) % 4096, 0);
  buffer.setSize(10'000);
  EXPECT_EQ(buffer.bytes().size(), 10'000);
  EXPECT_THROW(buffer.setSize(10'001), std::out_of_range);

  auto moved = std::move(buffer);
  EXPECT_EQ(moved.size(), 10'000);
}

TEST(BufferTest, wrap) {
  char memory[16] = "wrapped";
  auto buffer = Buffer::wrap(memory, sizeof(memory));
  EXPECT_EQ(buffer.data(), reinterpret_cast<uint8_t*>(memory));
  buffer.setSize(7);
  EXPECT_EQ(folly::StringPiece(buffer.bytes()), "wrapped");
}

} // namespace
} // namespace kiln
