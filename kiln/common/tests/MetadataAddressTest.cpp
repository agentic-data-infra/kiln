#include <gtest/gtest.h>

#include "kiln/common/Error.h"
#include "kiln/common/Types.h"

namespace kiln {
namespace {

TEST(MetadataAddressTest, parsesUrls) {
  const auto both = MetadataAddress::parse("tcp://meta:7470, rdma://10.0.0.1:7471");
  EXPECT_EQ(both.tcp, (Endpoint{"meta", 7470}));
  EXPECT_EQ(both.rdma, (Endpoint{"10.0.0.1", 7471}));
  EXPECT_EQ(both.toString(), "tcp://meta:7470,rdma://10.0.0.1:7471");
  EXPECT_EQ(MetadataAddress::parse(both.toString()), both);

  // Any order; a bare host:port is TCP.
  const auto bare = MetadataAddress::parse("rdma://10.0.0.1:7471,meta:7470");
  EXPECT_EQ(bare, both);

  const auto rdmaOnly = MetadataAddress::parse("rdma://10.0.0.1:7471");
  EXPECT_FALSE(rdmaOnly.tcp);
  EXPECT_EQ(rdmaOnly.toString(), "rdma://10.0.0.1:7471");

  EXPECT_EQ(MetadataAddress::parse("meta:7470"), MetadataAddress(Endpoint{"meta", 7470}));
}

TEST(MetadataAddressTest, rejectsBadUrls) {
  for (const char* urls :
       {"",
        ",",
        "http://meta:80",
        "meta",
        "tcp://meta:7470,meta:7471",
        "rdma://a:1,rdma://b:2"}) {
    try {
      MetadataAddress::parse(urls);
      ADD_FAILURE() << "Parsed '" << urls << "'";
    } catch (const Error& e) {
      EXPECT_EQ(e.code(), ErrorCode::kInvalidArgument) << urls;
    }
  }
}

} // namespace
} // namespace kiln
