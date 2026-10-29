// Memos across a restart of the cluster: memo_restart.sh runs this against
// a cluster with a journal, restarts the cluster, and runs it again.
//
//   memo --metadata=host:port put|get
//
// put computes two results and memos them: one whose data is written
// through to the local FS, and one whose data is only in DRAM. get looks
// both up and reads the data of hits; for a miss it recomputes the result
// and memos it again. After a restart, the first is still a hit, and the
// second a miss: its data was lost with DRAM.

#include <fmt/format.h>
#include <folly/coro/BlockingWait.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include "kiln/catalog/Catalog.h"
#include "kiln/client/Client.h"
#include "kiln/common/Error.h"

DEFINE_string(metadata, "localhost:7470", "The metadata server.");

namespace {

using namespace kiln;

struct Query {
  std::string key;
  Placement placement;
};

const Query kQueries[] = {
    {"select sum(x) from t -- written through",
     Placement{.tier = Tier::kDram, .writeThrough = Tier::kLocalFs}},
    {"select sum(x) from t -- in DRAM only", Placement{.tier = Tier::kDram}},
};

// "Computes" the query's result and stores it as one chunk.
folly::coro::Task<catalog::Manifest> co_compute(
    client::Client& client,
    const Query& query) {
  const auto result = fmt::format("result of '{}'\n", query.key);
  auto writer = co_await client.co_create(kDefaultBucket, query.placement);
  auto buffer = client.allocateBuffer(result.size());
  std::memcpy(buffer.data(), result.data(), result.size());
  buffer.setSize(result.size());
  co_await writer->co_append(std::move(buffer));
  const auto id = co_await writer->co_commit();
  co_return catalog::Manifest{
      .chunks = {catalog::ChunkRange{.id = id}},
      .schema = "line VARCHAR",
      .rows = 1,
      .bytes = result.size()};
}

folly::coro::Task<void> co_put(
    client::Client& client,
    catalog::Catalog& catalog) {
  for (const auto& query : kQueries) {
    auto manifest = co_await co_compute(client, query);
    const bool put = co_await catalog.co_putMemo(query.key, std::move(manifest));
    fmt::print("{}: {}\n", query.key, put ? "memo put" : "memo existed");
  }
}

folly::coro::Task<void> co_get(
    client::Client& client,
    catalog::Catalog& catalog) {
  for (const auto& query : kQueries) {
    auto manifest = co_await catalog.co_getMemo(query.key);
    if (!manifest) {
      fmt::print("{}: miss, recomputing\n", query.key);
      auto computed = co_await co_compute(client, query);
      co_await catalog.co_putMemo(query.key, std::move(computed));
      continue;
    }
    const auto& range = manifest->chunks[0];
    auto data = co_await client.co_read(
        range.id, 0, manifest->bytes, client.allocateBuffer(manifest->bytes));
    fmt::print(
        "{}: hit, {} row(s): {}",
        query.key,
        manifest->rows,
        std::string(reinterpret_cast<const char*>(data.data()), data.size()));
  }
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  const std::string step = argc == 2 ? argv[1] : "";
  if (step != "put" && step != "get") {
    fmt::print(stderr, "Usage: memo [--metadata=host:port] put|get\n");
    return 2;
  }
  try {
    client::Client client({.metadata = Endpoint::parse(FLAGS_metadata)});
    catalog::Catalog catalog(client);
    folly::coro::blockingWait(
        step == "put" ? co_put(client, catalog) : co_get(client, catalog));
  } catch (const Error& e) {
    fmt::print(stderr, "memo: {} ({})\n", e.what(), toString(e.code()));
    return 1;
  }
  return 0;
}
