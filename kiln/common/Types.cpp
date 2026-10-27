#include "kiln/common/Types.h"

#include <unistd.h>

#include <climits>

#include <fmt/format.h>
#include <folly/Conv.h>
#include <folly/String.h>

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

std::string MetadataAddress::toString() const {
  std::string urls;
  if (tcp) {
    urls = "tcp://" + tcp->toString();
  }
  if (rdma) {
    urls += (urls.empty() ? "rdma://" : ",rdma://") + rdma->toString();
  }
  return urls;
}

MetadataAddress MetadataAddress::parse(const std::string& urls) {
  MetadataAddress address;
  std::vector<folly::StringPiece> parts;
  folly::split(',', urls, parts);
  for (auto part : parts) {
    auto url = folly::trimWhitespace(part);
    auto* endpoint = &address.tcp;
    if (url.removePrefix("rdma://")) {
      endpoint = &address.rdma;
    } else {
      url.removePrefix("tcp://");
    }
    if (url.find("://") != folly::StringPiece::npos) {
      throw Error(
          ErrorCode::kInvalidArgument,
          fmt::format("Expected a tcp:// or rdma:// URL, got '{}'", part));
    }
    if (*endpoint) {
      throw Error(
          ErrorCode::kInvalidArgument,
          fmt::format("Two URLs of one scheme in '{}'", urls));
    }
    *endpoint = Endpoint::parse(url.str());
  }
  return address;
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
    case Tier::kObjectStore:
      return "objectstore";
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
  if (name == "objectstore") {
    return Tier::kObjectStore;
  }
  throw Error(
      ErrorCode::kInvalidArgument,
      fmt::format(
          "Unknown tier '{}', expected dram, localfs or objectstore", name));
}

std::string chunkObjectKey(const ChunkId& id) {
  return "chunks/" + id.toString();
}

} // namespace kiln
