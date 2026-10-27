#include "kiln/rdma/Device.h"

#include <ifaddrs.h>
#include <net/if.h>

#include <vector>

#include <fmt/format.h>

#include "kiln/common/Error.h"
#include "rlib/endpoint/RdmaEndpointProvider.h"
#include "rlib/verbs/RdmaCmId.h"
#include "rlib/verbs/RdmaError.h"
#include "rlib/verbs/RdmaEventChannel.h"
#include "rlib/verbs/SocketAddress.h"

namespace kiln::rdma {

namespace {

// An id bound to 'address' (port 0), if the address is on an RDMA device.
std::unique_ptr<rlib::RdmaCmId> bindToDevice(
    rlib::RdmaEventChannel& channel,
    const rlib::SocketAddress& address) {
  auto id = std::make_unique<rlib::RdmaCmId>(channel);
  try {
    id->bindAddr(address);
  } catch (const rlib::RdmaError&) {
    return nullptr;
  }
  return id->verbs() != nullptr ? std::move(id) : nullptr;
}

// The addresses of this host's IPv4 interfaces, loopback excluded.
std::vector<rlib::SocketAddress> interfaceAddresses() {
  std::vector<rlib::SocketAddress> addresses;
  ifaddrs* interfaces = nullptr;
  if (::getifaddrs(&interfaces) != 0) {
    return addresses;
  }
  for (auto* i = interfaces; i != nullptr; i = i->ifa_next) {
    if (i->ifa_addr != nullptr && i->ifa_addr->sa_family == AF_INET &&
        (i->ifa_flags & IFF_LOOPBACK) == 0) {
      addresses.push_back(rlib::SocketAddress::fromSockaddr(i->ifa_addr));
    }
  }
  ::freeifaddrs(interfaces);
  return addresses;
}

} // namespace

Device Device::open(const std::string& host) {
  try {
    auto channel = std::make_unique<rlib::RdmaEventChannel>();
    std::vector<rlib::SocketAddress> candidates;
    if (host.empty()) {
      candidates = interfaceAddresses();
    } else {
      candidates.push_back(rlib::SocketAddress::parse(host + ":0"));
    }
    for (const auto& address : candidates) {
      if (auto id = bindToDevice(*channel, address)) {
        return Device(std::move(channel), std::move(id), address.host());
      }
    }
  } catch (const rlib::RdmaError& e) {
    throw Error(
        ErrorCode::kUnavailable,
        fmt::format("Cannot open the RDMA device of {}: {}", host, e.what()));
  }
  throw Error(
      ErrorCode::kUnavailable,
      host.empty() ? std::string("No interface has an RDMA device")
                   : fmt::format("{} is not on an RDMA device", host));
}

Device::Device(
    std::unique_ptr<rlib::RdmaEventChannel> channel,
    std::unique_ptr<rlib::RdmaCmId> id,
    std::string host)
    : channel_{std::move(channel)},
      id_{std::move(id)},
      host_{std::move(host)},
      pd_{rlib::RdmaEndpointProvider::get().createProtectionDomain(
          id_->verbs())} {}

Device::Device(Device&&) noexcept = default;
Device& Device::operator=(Device&&) noexcept = default;

Device::~Device() = default;

std::string Device::name() const {
  return ::ibv_get_device_name(context()->device);
}

ibv_context* Device::context() const {
  return id_->verbs();
}

rlib::MemoryRegion Device::registerMemory(void* data, size_t length) const {
  try {
    return rlib::MemoryRegion{pd_, data, length};
  } catch (const rlib::RdmaError& e) {
    throw Error(
        ErrorCode::kUnavailable,
        fmt::format(
            "Cannot register {} bytes with {}: {}", length, name(), e.what()));
  }
}

} // namespace kiln::rdma
