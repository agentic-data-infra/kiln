#pragma once

#include <stdexcept>
#include <string>

namespace kiln {

enum class ErrorCode {
  // No such chunk.
  kNotFound,
  // No free blocks in the requested tier.
  kOutOfSpace,
  // A call's arguments are wrong, e.g. a range outside a region.
  kInvalidArgument,
  // A server cannot be reached, or the connection to it broke.
  kUnavailable,
  kInternal,
  // A call's arguments or result do not fit the transport's messages (RDMA
  // metadata calls, whose messages have a fixed size).
  kTooLarge,
};

const char* toString(ErrorCode code);

// What Kiln's calls fail with, on clients and servers alike. Errors that
// servers throw reach clients with their code and message.
class Error : public std::runtime_error {
 public:
  Error(ErrorCode code, const std::string& message)
      : std::runtime_error{message}, code_{code} {}

  ErrorCode code() const {
    return code_;
  }

 private:
  ErrorCode code_;
};

} // namespace kiln
