#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include <folly/io/IOBuf.h>

#include "kiln/common/Types.h"

namespace kiln::server {

// A storage server's region: 'length' bytes in a file at 'path', created when
// the region is and removed with it. A DRAM region's file is in /dev/shm and
// mapped into memory; a local-FS region's file is preallocated on disk and
// accessed with pread/pwrite. Clients on the same host map the file
// directly (ShmAccess). Thread safe.
class Region {
 public:
  // Throws Error(kUnavailable) if the file cannot be created or sized.
  Region(Tier tier, std::string path, uint64_t length);

  ~Region();

  Region(const Region&) = delete;
  Region& operator=(const Region&) = delete;

  Tier tier() const {
    return tier_;
  }

  const std::string& path() const {
    return path_;
  }

  uint64_t length() const {
    return length_;
  }

  // Throw Error(kInvalidArgument) for a range outside the region.
  std::unique_ptr<folly::IOBuf> read(uint64_t offset, uint64_t length) const;
  void write(uint64_t offset, const folly::IOBuf& data);

 private:
  void checkRange(uint64_t offset, uint64_t length) const;

  const Tier tier_;
  const std::string path_;
  const uint64_t length_;
  int fd_{-1};
  // The mapping of a DRAM region.
  uint8_t* memory_{nullptr};
};

} // namespace kiln::server
