#include "kiln/meta/Journal.h"

#include <fcntl.h>
#include <unistd.h>

#include <cstring>

#include <fmt/format.h>
#include <folly/FileUtil.h>
#include <folly/hash/Checksum.h>

#include "kiln/common/Error.h"

namespace kiln::meta {

namespace {

// Each record is framed as its size and CRC32C, then its bytes.
constexpr size_t kHeaderSize = 8;

[[noreturn]] void fail(const std::string& what, const std::string& path) {
  throw Error(
      ErrorCode::kInternal,
      fmt::format("Cannot {} journal {}: {}", what, path, strerror(errno)));
}

void appendFramed(std::string& out, folly::ByteRange record) {
  const uint32_t header[2] = {
      static_cast<uint32_t>(record.size()),
      folly::crc32c(record.data(), record.size())};
  out.append(reinterpret_cast<const char*>(header), sizeof(header));
  out.append(reinterpret_cast<const char*>(record.data()), record.size());
}

void writeAndSync(int fd, const std::string& data, const std::string& path) {
  if (folly::writeFull(fd, data.data(), data.size()) !=
          static_cast<ssize_t>(data.size()) ||
      ::fdatasync(fd) != 0) {
    fail("write", path);
  }
}

} // namespace

Journal::Journal(
    std::string path,
    const std::function<void(folly::ByteRange)>& replay)
    : path_{std::move(path)} {
  std::string data;
  if (!folly::readFile(path_.c_str(), data) && errno != ENOENT) {
    fail("read", path_);
  }
  size_t offset = 0;
  while (data.size() - offset >= kHeaderSize) {
    uint32_t header[2];
    memcpy(header, data.data() + offset, sizeof(header));
    if (data.size() - offset - kHeaderSize < header[0]) {
      break;
    }
    folly::ByteRange record(
        reinterpret_cast<const uint8_t*>(data.data()) + offset + kHeaderSize,
        header[0]);
    if (folly::crc32c(record.data(), record.size()) != header[1]) {
      break;
    }
    replay(record);
    offset += kHeaderSize + header[0];
  }
  openForAppend();
  if (offset < data.size() && ::ftruncate(fd_, offset) != 0) {
    fail("truncate", path_);
  }
}

Journal::~Journal() {
  if (fd_ >= 0) {
    ::close(fd_);
  }
}

void Journal::openForAppend() {
  fd_ = ::open(path_.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
  if (fd_ < 0) {
    fail("open", path_);
  }
}

void Journal::append(folly::ByteRange record) {
  std::string framed;
  appendFramed(framed, record);
  writeAndSync(fd_, framed, path_);
}

void Journal::rewrite(const std::vector<std::string>& records) {
  std::string data;
  for (const auto& record : records) {
    appendFramed(data, folly::StringPiece(record));
  }
  const auto temp = path_ + ".new";
  const int fd =
      ::open(temp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) {
    fail("create", temp);
  }
  writeAndSync(fd, data, temp);
  ::close(fd);
  if (::rename(temp.c_str(), path_.c_str()) != 0) {
    fail("replace", path_);
  }
  // Make the rename itself durable.
  const auto slash = path_.rfind('/');
  const auto dir = slash == std::string::npos ? std::string(".")
                                              : path_.substr(0, slash + 1);
  const int dirFd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (dirFd < 0 || ::fsync(dirFd) != 0) {
    fail("sync the directory of", path_);
  }
  ::close(dirFd);
  ::close(fd_);
  openForAppend();
}

RecordWriter& RecordWriter::u8(uint8_t value) {
  record_.push_back(static_cast<char>(value));
  return *this;
}

RecordWriter& RecordWriter::u64(uint64_t value) {
  record_.append(reinterpret_cast<const char*>(&value), sizeof(value));
  return *this;
}

RecordWriter& RecordWriter::string(folly::StringPiece value) {
  u64(value.size());
  record_.append(value.data(), value.size());
  return *this;
}

folly::ByteRange RecordReader::take(size_t size) {
  if (record_.size() < size) {
    throw Error(ErrorCode::kInternal, "Journal record is too short");
  }
  auto result = record_.subpiece(0, size);
  record_.advance(size);
  return result;
}

uint8_t RecordReader::u8() {
  return take(1)[0];
}

uint64_t RecordReader::u64() {
  uint64_t value;
  memcpy(&value, take(sizeof(value)).data(), sizeof(value));
  return value;
}

std::string RecordReader::string() {
  auto bytes = take(u64());
  return std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
}

} // namespace kiln::meta
