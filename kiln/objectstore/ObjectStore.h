// Whole objects by key, as in S3. Kiln uses an object store as its durable
// tier (later) and for the files its metadata server keeps.

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <folly/Range.h>
#include <folly/coro/Task.h>

namespace kiln::objectstore {

// Keys are relative paths: non-empty '/'-separated names, none of them
// empty, "." or "..". Puts replace objects whole, so readers see an object
// as it was before or after a put, never in between. Calls fail with
// kiln::Error: kNotFound for no such object, kInvalidArgument for a bad key.
class ObjectStore {
 public:
  virtual ~ObjectStore() = default;

  // Stores 'data' as the object 'key', replacing it if it exists.
  virtual folly::coro::Task<void> co_put(
      std::string key,
      folly::ByteRange data) = 0;

  // Reads up to dst.size() bytes of the object at 'offset' into 'dst';
  // returns how many (fewer at the end of the object).
  virtual folly::coro::Task<size_t>
  co_get(std::string key, uint64_t offset, folly::MutableByteRange dst) = 0;

  // The object's size, or nullopt if there is no such object.
  virtual folly::coro::Task<std::optional<uint64_t>> co_head(
      std::string key) = 0;

  // The keys that start with 'prefix', sorted.
  virtual folly::coro::Task<std::vector<std::string>> co_list(
      std::string prefix) = 0;

  // Returns whether the object existed.
  virtual folly::coro::Task<bool> co_remove(std::string key) = 0;
};

// Objects as files under 'root', which is created if missing. A put writes
// a temporary file under root/.tmp and renames it into place.
std::unique_ptr<ObjectStore> makeLocalObjectStore(std::string root);

} // namespace kiln::objectstore
