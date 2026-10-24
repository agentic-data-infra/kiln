#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include <folly/Range.h>

namespace kiln::meta {

// An append-only log of records in a file, for the metadata server's state
// that must outlive it. Each record is synced to disk before append returns.
// Not thread safe.
class Journal {
 public:
  // Opens the journal at 'path', creating it if there is none, and calls
  // 'replay' with each record in order. A torn record at the end, from a
  // crash during an append, is cut off. Throws Error(kInternal) if the file
  // cannot be read or written.
  Journal(
      std::string path,
      const std::function<void(folly::ByteRange)>& replay);

  ~Journal();

  Journal(const Journal&) = delete;
  Journal& operator=(const Journal&) = delete;

  void append(folly::ByteRange record);

  // Replaces all records at once with 'records', e.g. fewer that give the
  // same state.
  void rewrite(const std::vector<std::string>& records);

 private:
  void openForAppend();

  const std::string path_;
  int fd_{-1};
};

// Builds a record from integers and strings.
class RecordWriter {
 public:
  RecordWriter& u8(uint8_t value);
  RecordWriter& u64(uint64_t value);
  RecordWriter& string(folly::StringPiece value);

  const std::string& record() const {
    return record_;
  }

 private:
  std::string record_;
};

// Reads a RecordWriter's record back. Throws Error(kInternal) if the record
// is shorter than what is read.
class RecordReader {
 public:
  explicit RecordReader(folly::ByteRange record) : record_{record} {}

  uint8_t u8();
  uint64_t u64();
  std::string string();

  bool done() const {
    return record_.empty();
  }

 private:
  folly::ByteRange take(size_t size);

  folly::ByteRange record_;
};

} // namespace kiln::meta
