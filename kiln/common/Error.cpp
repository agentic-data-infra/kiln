#include "kiln/common/Error.h"

namespace kiln {

const char* toString(ErrorCode code) {
  switch (code) {
    case ErrorCode::kNotFound:
      return "not found";
    case ErrorCode::kOutOfSpace:
      return "out of space";
    case ErrorCode::kInvalidArgument:
      return "invalid argument";
    case ErrorCode::kUnavailable:
      return "unavailable";
    case ErrorCode::kInternal:
      return "internal";
  }
  return "unknown";
}

} // namespace kiln
