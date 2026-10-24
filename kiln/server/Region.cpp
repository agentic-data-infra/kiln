#include "kiln/server/Region.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include <fmt/format.h>
#include <glog/logging.h>

#include "kiln/common/Error.h"

namespace kiln::server {

namespace {

[[noreturn]] void throwSystemError(const std::string& what, int error) {
  throw Error(
      ErrorCode::kUnavailable,
      fmt::format("{}: {}", what, std::strerror(error)));
}

} // namespace

Region::Region(Tier tier, std::string path, uint64_t length, bool keep)
    : tier_{tier}, path_{std::move(path)}, length_{length}, keep_{keep} {
  fd_ = ::open(
      path_.c_str(),
      O_RDWR | O_CREAT | (keep_ ? 0 : O_EXCL) | O_CLOEXEC,
      0600);
  struct stat st;
  if (fd_ < 0 || ::fstat(fd_, &st) != 0) {
    throwSystemError(fmt::format("Cannot create {}", path_), errno);
  }
  if (st.st_size > 0) {
    // Reattached.
    length_ = static_cast<uint64_t>(st.st_size);
  }
  // Reserve the space now, so a full disk or /dev/shm fails here and not on
  // a later write.
  if (int error = ::posix_fallocate(fd_, 0, static_cast<off_t>(length_))) {
    ::close(fd_);
    if (!keep_) {
      ::unlink(path_.c_str());
    }
    throwSystemError(
        fmt::format("Cannot allocate {} bytes for {}", length_, path_), error);
  }
  if (tier_ == Tier::kDram) {
    void* memory =
        ::mmap(nullptr, length_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
    if (memory == MAP_FAILED) {
      const int error = errno;
      ::close(fd_);
      ::unlink(path_.c_str());
      throwSystemError(fmt::format("Cannot map {}", path_), error);
    }
    memory_ = static_cast<uint8_t*>(memory);
  }
}

Region::~Region() {
  if (memory_ != nullptr) {
    ::munmap(memory_, length_);
  }
  ::close(fd_);
  if (!keep_ && ::unlink(path_.c_str()) != 0) {
    PLOG(WARNING) << "Cannot remove " << path_;
  }
}

void Region::checkRange(uint64_t offset, uint64_t length) const {
  if (offset > length_ || length > length_ - offset) {
    throw Error(
        ErrorCode::kInvalidArgument,
        fmt::format(
            "Range {}+{} is outside the region of {} bytes",
            offset,
            length,
            length_));
  }
}

std::unique_ptr<folly::IOBuf> Region::read(uint64_t offset, uint64_t length)
    const {
  checkRange(offset, length);
  auto data = folly::IOBuf::create(length);
  if (memory_ != nullptr) {
    std::memcpy(data->writableData(), memory_ + offset, length);
  } else {
    uint64_t done = 0;
    while (done < length) {
      auto n = ::pread(
          fd_,
          data->writableData() + done,
          length - done,
          static_cast<off_t>(offset + done));
      if (n < 0 && errno == EINTR) {
        continue;
      }
      if (n <= 0) {
        throwSystemError(fmt::format("Cannot read {}", path_), n < 0 ? errno : EIO);
      }
      done += n;
    }
  }
  data->append(length);
  return data;
}

void Region::write(uint64_t offset, const folly::IOBuf& data) {
  checkRange(offset, data.computeChainDataLength());
  for (auto range : data) {
    if (memory_ != nullptr) {
      std::memcpy(memory_ + offset, range.data(), range.size());
    } else {
      uint64_t done = 0;
      while (done < range.size()) {
        auto n = ::pwrite(
            fd_,
            range.data() + done,
            range.size() - done,
            static_cast<off_t>(offset + done));
        if (n < 0 && errno == EINTR) {
          continue;
        }
        if (n <= 0) {
          throwSystemError(
              fmt::format("Cannot write {}", path_), n < 0 ? errno : EIO);
        }
        done += n;
      }
    }
    offset += range.size();
  }
}

} // namespace kiln::server
