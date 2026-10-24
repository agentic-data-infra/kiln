// Internal to kiln/rpc: Kiln's types to and from the generated ones.

#pragma once

#include <optional>
#include <vector>

#include "kiln/common/Error.h"
#include "kiln/common/Types.h"
#include "kiln/rpc/gen-cpp2/kiln_types.h"

namespace kiln::rpc {

thrift::Endpoint toThrift(const Endpoint& endpoint);
Endpoint fromThrift(const thrift::Endpoint& endpoint);

thrift::RegionInfo toThrift(const RegionInfo& region);
RegionInfo fromThrift(const thrift::RegionInfo& region);

thrift::BlockRef toThrift(const BlockRef& block);
BlockRef fromThrift(const thrift::BlockRef& block);

thrift::ChunkId toThrift(const ChunkId& id);
ChunkId fromThrift(const thrift::ChunkId& id);

thrift::Placement toThrift(const Placement& placement);
Placement fromThrift(const thrift::Placement& placement);

thrift::Replica toThrift(const Replica& replica);
Replica fromThrift(const thrift::Replica& replica);

thrift::ChunkInfo toThrift(const ChunkInfo& chunk);
ChunkInfo fromThrift(const thrift::ChunkInfo& chunk);

thrift::Ref toThrift(const Ref& ref);
Ref fromThrift(const thrift::Ref& ref);

thrift::RefLookup toThrift(const std::optional<Ref>& ref);
std::optional<Ref> fromThrift(const thrift::RefLookup& lookup);

thrift::KilnException toThrift(const Error& error);
Error fromThrift(const thrift::KilnException& error);

template <typename T>
auto toThrift(const std::vector<T>& items) {
  std::vector<decltype(toThrift(items[0]))> result;
  result.reserve(items.size());
  for (const auto& item : items) {
    result.push_back(toThrift(item));
  }
  return result;
}

template <typename T>
auto fromThrift(const std::vector<T>& items) {
  std::vector<decltype(fromThrift(items[0]))> result;
  result.reserve(items.size());
  for (const auto& item : items) {
    result.push_back(fromThrift(item));
  }
  return result;
}

} // namespace kiln::rpc
