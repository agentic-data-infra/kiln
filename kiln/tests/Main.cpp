// Test main for Kiln tests: folly must be initialized before any test runs.

#include <folly/init/Init.h>
#include <gtest/gtest.h>

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
