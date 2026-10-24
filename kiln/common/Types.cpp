#include "kiln/common/Types.h"

#include <unistd.h>

#include <climits>

#include <fmt/format.h>
#include <folly/Conv.h>

#include "kiln/common/Error.h"

namespace kiln {

std::string Endpoint::toString() const {
  if (host.find(':') != std::string::npos) {
    return fmt::format("[{}]:{}", host, port);
  }
  return fmt::format("{}:{}", host, port);
}

Endpoint Endpoint::parse(const std::string& endpoint) {
  const auto colon = endpoint.rfind(':');
  if (colon == std::string::npos || colon == 0 ||
      colon + 1 == endpoint.size()) {
    throw Error(
        ErrorCode::kInvalidArgument,
        fmt::format("Expected host:port, got '{}'", endpoint));
  }
  auto host = endpoint.substr(0, colon);
  if (host.size() >= 2 && host.front() == '[' && host.back() == ']') {
    host = host.substr(1, host.size() - 2);
  }
  auto port = folly::tryTo<uint16_t>(endpoint.substr(colon + 1));
  if (!port.hasValue()) {
    throw Error(
        ErrorCode::kInvalidArgument,
        fmt::format("Bad port in '{}'", endpoint));
  }
  return Endpoint{std::move(host), *port};
}

std::string localHostName() {
  char name[HOST_NAME_MAX + 1] = {};
  if (::gethostname(name, sizeof(name)) != 0 || name[0] == '\0') {
    return "localhost";
  }
  return name;
}

const char* toString(Tier tier) {
  switch (tier) {
    case Tier::kDram:
      return "dram";
    case Tier::kLocalFs:
      return "localfs";
  }
  return "unknown";
}

Tier parseTier(const std::string& name) {
  if (name == "dram") {
    return Tier::kDram;
  }
  if (name == "localfs") {
    return Tier::kLocalFs;
  }
  throw Error(
      ErrorCode::kInvalidArgument,
      fmt::format("Unknown tier '{}', expected dram or localfs", name));
}

} // namespace kiln
