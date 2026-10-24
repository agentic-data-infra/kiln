#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <filesystem>

#include <fmt/format.h>
#include <folly/FileUtil.h>
#include <folly/Random.h>

#include "kiln/common/Error.h"
#include "kiln/objectstore/ObjectStore.h"

namespace kiln::objectstore {

namespace {

namespace fs = std::filesystem;

constexpr const char* kTempDir = ".tmp";

void checkKey(const std::string& key) {
  bool ok = !key.empty() && key.front() != '/' && key.back() != '/' &&
      key.find('\0') == std::string::npos;
  for (size_t begin = 0; ok && begin < key.size();) {
    auto end = key.find('/', begin);
    if (end == std::string::npos) {
      end = key.size();
    }
    const auto name = std::string_view(key).substr(begin, end - begin);
    ok = !name.empty() && name != "." && name != "..";
    begin = end + 1;
  }
  if (!ok || key == kTempDir || key.starts_with(std::string(kTempDir) + "/")) {
    throw Error(ErrorCode::kInvalidArgument, fmt::format("Bad key '{}'", key));
  }
}

[[noreturn]] void fail(const std::string& what, const std::string& path) {
  throw Error(
      ErrorCode::kInternal,
      fmt::format("Cannot {} {}: {}", what, path, strerror(errno)));
}

class LocalObjectStore : public ObjectStore {
 public:
  explicit LocalObjectStore(std::string root) : root_{std::move(root)} {
    std::error_code error;
    fs::create_directories(root_ / kTempDir, error);
    if (error) {
      throw Error(
          ErrorCode::kInternal,
          fmt::format("Cannot create {}: {}", root_.string(), error.message()));
    }
  }

  folly::coro::Task<void> co_put(std::string key, folly::ByteRange data)
      override {
    checkKey(key);
    const auto path = root_ / key;
    const auto temp =
        (root_ / kTempDir / fmt::format("{:016x}", folly::Random::rand64()))
            .string();
    const int fd =
        ::open(temp.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
    if (fd < 0) {
      fail("create", temp);
    }
    const bool written =
        folly::writeFull(fd, data.data(), data.size()) ==
            static_cast<ssize_t>(data.size()) &&
        ::fdatasync(fd) == 0;
    ::close(fd);
    std::error_code error;
    if (written) {
      fs::create_directories(path.parent_path(), error);
    }
    if (!written || error ||
        ::rename(temp.c_str(), path.string().c_str()) != 0) {
      ::unlink(temp.c_str());
      fail("write", path.string());
    }
    co_return;
  }

  folly::coro::Task<size_t>
  co_get(std::string key, uint64_t offset, folly::MutableByteRange dst)
      override {
    checkKey(key);
    const auto path = (root_ / key).string();
    const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    struct stat st;
    if (fd < 0 || ::fstat(fd, &st) != 0 || !S_ISREG(st.st_mode)) {
      const bool missing = fd >= 0 || errno == ENOENT || errno == ENOTDIR;
      if (fd >= 0) {
        ::close(fd);
      }
      if (missing) {
        throw Error(ErrorCode::kNotFound, fmt::format("No object '{}'", key));
      }
      fail("open", path);
    }
    const auto n = folly::preadFull(fd, dst.data(), dst.size(), offset);
    ::close(fd);
    if (n < 0) {
      fail("read", path);
    }
    co_return static_cast<size_t>(n);
  }

  folly::coro::Task<std::optional<uint64_t>> co_head(std::string key)
      override {
    checkKey(key);
    std::error_code error;
    const auto path = root_ / key;
    if (!fs::is_regular_file(path, error)) {
      co_return std::nullopt;
    }
    co_return fs::file_size(path);
  }

  folly::coro::Task<std::vector<std::string>> co_list(std::string prefix)
      override {
    std::vector<std::string> keys;
    std::error_code error;
    for (auto it = fs::recursive_directory_iterator(root_, error);
         it != fs::recursive_directory_iterator();
         it.increment(error)) {
      if (it->path().filename() == kTempDir && it.depth() == 0) {
        it.disable_recursion_pending();
        continue;
      }
      if (it->is_regular_file()) {
        auto key = fs::relative(it->path(), root_).string();
        if (key.starts_with(prefix)) {
          keys.push_back(std::move(key));
        }
      }
    }
    std::sort(keys.begin(), keys.end());
    co_return keys;
  }

  folly::coro::Task<bool> co_remove(std::string key) override {
    checkKey(key);
    std::error_code error;
    const auto path = root_ / key;
    if (!fs::is_regular_file(path, error)) {
      co_return false;
    }
    co_return fs::remove(path, error);
  }

 private:
  const fs::path root_;
};

} // namespace

std::unique_ptr<ObjectStore> makeLocalObjectStore(std::string root) {
  return std::make_unique<LocalObjectStore>(std::move(root));
}

} // namespace kiln::objectstore
